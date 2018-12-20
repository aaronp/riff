package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging
import eie.io.{FromBytes, ToBytes}
import io.circe.{Decoder, Encoder}
import io.vertx.core.Handler
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.http.HttpServerRequest
import io.vertx.scala.ext.web.{Router, RoutingContext}
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import monix.reactive.{Consumer, Observable}
import riff.json.LowPriorityRiffJsonImplicits
import riff.monix.{LowPriorityRiffMonixImplicits, RaftMonix, RiffSchedulers}
import riff.raft.messages.RaftMessage
import riff.raft.{NodeId, RaftClient}
import riff.vertx.client.SocketClient
import riff.vertx.server.Server.{LoggingHandler, OnConnect}
import riff.vertx.server.{RoutingSocketHandler, Server, ServerEndpoint, ServerWebSocketHandler}
import riff.rest.sockets.{TextFrame, WebFrame}
import riff.rest.HostPort
import riff.rest.{Endpoint, EndpointCoords, HostPort, WebURI}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

/**
  * Provides an entry-point for starting a vertx service which can connect a RaftNode
  */
object RunningVertxService extends LowPriorityRiffJsonImplicits with LowPriorityRiffMonixImplicits with StrictLogging {

  /**
    * Starts a Vertx web service
    *
    * @param args the user agrs
    * @return a running vertx services
    */
  def start(args: Array[String], portOffset: Int): Option[RunningVertxService[String]] = {
    implicit val scheduler = delegateScheduler
    VertxClusterConfig.fromArgs(args, portOffset).map { config =>
      implicit val vertx = config.vertx
      val running        = RunningVertxService[String](config)

      logger.info(s"Started ${running.config.name}")

      import running.raft

      raft.timerCallback.receiveTimeouts.foreach { _ => //
        logger.info(s"${config.name} got received HB timeout")
      }
      raft.timerCallback.sendTimeout.foreach { _ => //
        logger.info(s"${config.name} got send HB timeout")
      }
      raft.stateCallback.events.foreach { event => //
        logger.info(s"${config.name} noticed $event")
      }
      raft.log.appendResults().foreach { event => //
        logger.info(s"${config.name} log appended $event")
      }
      raft.log.committedEntries().foreach { event => //
        logger.info(s"${config.name} log committed $event")
      }

      running
    }
  }

  def delegateScheduler(): RiffSchedulers.Delegate = {
    new RiffSchedulers.Delegate(RiffSchedulers.computation.newScheduler()) {
      override def shutdown(): Unit = {
        logger.warn("shutting down scheduler")
        super.shutdown()
      }
    }
  }

  /**
    * When we start up, we try to connect a socket to each of our peers. We don't need to re-try, as each peer will do
    * the same when it starts up
    *
    * @param builder
    * @param sched
    * @param socketTimeout
    * @return a map of node IDs to clients
    */
  def connectToPeers[A: Encoder: Decoder: ClassTag](builder: RaftMonix[A], capacity: Int)(implicit socketTimeout: FiniteDuration, vertx: Vertx): Map[NodeId, SocketClient] = {
    import builder.scheduler

    val cluster = builder.cluster
    val name    = builder.nodeId
    val pears = cluster.peers.map { peerHostPortString =>
      val peerHostPort = HostPort.unapply(peerHostPortString).getOrElse(sys.error(s"Couldn't parse peer as host:port: $peerHostPortString"))
      val endpoint     = EndpointCoords(peerHostPort, WebURI.post(name))
      logger.info(s"Connecting to $peerHostPortString at $endpoint")
      val client = SocketClient.connect(endpoint, capacity, name) { endpoint => //
        connectClientToPeer(builder, peerHostPort, endpoint)
      }
      (peerHostPortString, client)
    }
    pears.toMap.ensuring(_.size == pears.size)
  }

  private def connectClientToPeer[A: Encoder: Decoder: ClassTag](raft: RaftMonix[A], peer: HostPort, endpoint: Endpoint[WebFrame, WebFrame]): Unit = {

    implicit val sched = raft.scheduler

    // subscribe our node to this stream
    val fromMessages: Observable[RaftMessage[A]] = endpoint.fromRemote.flatMap { frame =>
      val json = frame.asText.getOrElse(sys.error("binary frames not supported"))
      import io.circe.parser._
      decode[RaftMessage[A]](json) match {
        case Left(err) =>
          logger.error(s"Couldn't unmarshall: $err:\n$json")
          Observable.empty
        case Right(msg: RaftMessage[A]) => Observable(msg)
      }
    }

    fromMessages.subscribe(raft.pipe.input)

    // subscribe the client's input to this node's output
    val nodeOutputForClient: Observable[TextFrame] = raft.pipe.inputFor(peer.hostPort).map { msg =>
      import io.circe.syntax._
      val json = msg.asJson
      WebFrame.text(json.noSpaces)
    }

    nodeOutputForClient.subscribe(endpoint.toRemote)
  }
}

/**
  * As opposed to composition, this class is meant to contain all the started pieces and expose them as properties for
  * access.
  *
  * That is, instead of something like this{
  *
  * {{{
  *   def start(config : SomeConfig) = {
  *      val foo = createFoo(config)
  *      val bar = createBar(config, foo)
  *      val bazz = createBazz(bar, ...)
  *
  *      new StartedThing(foo, bazz)
  *   }
  * }}}
  *
  * we have:
  *
  * {{{
  *   class StartedThing(config : SomeConfig) = {
  *      val foo = createFoo(config)
  *      private val bar = createBar(config, foo)
  *      val bazz = createBazz(bar, ...)
  *   }
  * }}}
  *
  * This makes it easier to access composed elements at different levels of a stack of parameters w/o having to relax
  * visibilities or go to other lengths.
  *
  * @param config
  * @param classTag$A
  * @param toBytes$A
  * @param fromBytes$A
  * @param encoder$A
  * @param decoder$A
  * @param scheduler
  * @param vertx
  * @tparam A
  */
case class RunningVertxService[A: ClassTag: ToBytes: FromBytes: Encoder: Decoder](config: VertxClusterConfig)(implicit val scheduler: Scheduler, vertx: Vertx)
    extends AutoCloseable with StrictLogging {

  implicit private val socketTimeout = config.socketTimeout
  val raft                           = config.mkNode[A]
  val cluster                        = config.cluster
  val hostPort                       = config.hostPort

  logger.info(s"Starting the server on ${hostPort} for $cluster")

  val websocketHandler: RoutingSocketHandler = {
    // try to connect to the other services
    val peerNames          = cluster.peers.toSet
    val StripForwardSlashR = "/?(.*)".r

    // the server-side logic for when a client connects on a web socket
    val onConnect: PartialFunction[String, OnConnect] = {
      case StripForwardSlashR(HostPort(peer)) =>
        (newClientWebsocketConnection: ServerEndpoint) =>
          logger.info(s"Client connecting from $peer")
          RunningVertxService.connectClientToPeer(raft, peer, newClientWebsocketConnection)
      case unknown =>
        (endpoint: ServerEndpoint) =>
          endpoint.fromRemote.consumeWith(Consumer.cancel)
          endpoint.toRemote.onError(new Exception(s"Invalid path '$unknown'. Expected one of ${peerNames.toList.sorted.mkString(",")}"))
    }

    RoutingSocketHandler(onConnect.andThen(ServerWebSocketHandler("general", config.socketConnectionMessageCapacity)))
  }

  val restHandler: Handler[HttpServerRequest] = {
    val route: (WebURI, Handler[RoutingContext]) = config.staticPath match {
      case None       => WebURI.get("/*") -> LoggingHandler
      case Some(path) => Server.asStaticHandler(path)
    }
    Server.makeHandler(Router.router(vertx), route :: Nil)
  }

  val verticle: ScalaVerticle = Server.start(hostPort, restHandler, websocketHandler)

  logger.info(s"Trying to connect to peers...")
  val clients: Map[NodeId, SocketClient] = RunningVertxService.connectToPeers(raft, config.socketConnectionMessageCapacity)

  logger.info(s"Handling messages on ${hostPort} for $cluster")

  // this isn't JUST debug -- with zero subscriptions the node doesn't do anything.
  // we need at least one subscriber to actually make the node do work
  raft.pipe.output.foreach { res =>
    logger.info(s"${raft.nodeId} sending $res")
  }

  // trigger the heart-beat mechanism
  raft.resetReceiveHeartbeat()

  def client: RaftClient[Observable, A] = raft.pipe.client

  override def close(): Unit = {
    shutdown()
  }

  def shutdown(): Future[Unit] = {
    raft.cancelHeartbeats()
    scheduler match {
      case ss: SchedulerService => ss.shutdown()
      case _                    =>
    }
    clients.values.foreach(_.close())
    vertx.closeFuture()
  }
}
