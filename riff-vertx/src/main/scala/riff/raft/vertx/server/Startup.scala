package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.circe.{Decoder, Encoder}
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import riff.json.LowPriorityRiffJsonImplicits
import riff.monix.RaftMonix
import riff.raft.NodeId
import riff.raft.messages.RaftMessage
import riff.vertx.client.SocketClient
import riff.vertx.server.{Server, ServerEndpoint}
import streaming.api.sockets.WebFrame
import streaming.api.{Endpoint, HostPort}
import streaming.rest.{EndpointCoords, WebURI}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object Startup extends StrictLogging with LowPriorityRiffJsonImplicits {

  /**
    * Starts a vertx web server at the given hostPort, serving data from the staticPath (if non-empty) and routing websocket connections
    * to the raft endpoint
    *
    * @param builder
    * @param hostPort
    * @param staticPath
    * @param sched
    * @param socketTimeout
    * @param vertx
    * @return
    */
  def startServer[A: Encoder: Decoder : ClassTag](raft: RaftMonix[A], hostPort: HostPort, staticPath: Option[String])(implicit sched: Scheduler,
                                                                                                           socketTimeout: FiniteDuration,
                                                                                                           vertx: Vertx): ScalaVerticle = {
    val cluster = raft.cluster

    // try to connect to the other services
    val peerNames = cluster.peers.toSet

    Server.startSocket(hostPort, staticPath) {
      case HostPort(peer) =>
        (newClientWebsocketConnection: ServerEndpoint) =>
          connectClientToPeer(raft, peer, newClientWebsocketConnection)
      case unknown =>
        (endpoint: ServerEndpoint) =>
          endpoint.fromRemote.consumeWith(Consumer.cancel)
          endpoint.toRemote.onError(new Exception(s"Invalid path '$unknown'. Expected one of ${peerNames.toList.sorted.mkString(",")}"))
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
  def connectToPeers[A: Encoder: Decoder: ClassTag](builder: RaftMonix[A])(implicit socketTimeout: FiniteDuration, vertx: Vertx): Map[NodeId, SocketClient] = {
    import builder.scheduler

    val cluster = builder.cluster
    val name    = builder.nodeId
    val pears = cluster.peers.map { peerHostPortString =>
      val peerHostPort = HostPort.unapply(peerHostPortString).getOrElse(sys.error(s"Couldn't parse peer as host:port: $peerHostPortString"))
      val endpoint     = EndpointCoords(peerHostPort, WebURI.post(name))
      logger.info(s"Connecting to $peerHostPortString at $endpoint")
      val client = SocketClient.connect(endpoint, name) { endpoint => //
        connectClientToPeer(builder, peerHostPort, endpoint)
      }
      (peerHostPortString, client)
    }
    pears.toMap.ensuring(_.size == pears.size)
  }

  private def connectClientToPeer[A: Encoder: Decoder : ClassTag](raft: RaftMonix[A], peer: HostPort, endpoint: Endpoint[WebFrame, WebFrame]): Unit = {

    import raft.scheduler
    val fromMessages: Observable[RaftMessage[A]] = endpoint.fromRemote.flatMap { frame =>
      val json = frame.asText.getOrElse(sys.error("binary frames not supported"))
      import io.circe.parser._
      decode[RaftMessage[A]](json) match {
        case Left(err) =>
          logger.error(s"Couldn't unmarshall: $err:\n$json")
          Observable.empty
        case Right(msg) => Observable(msg)
      }
    }

    // subscribe our node to this stream
    fromMessages.dump(s"---- FROM ${peer} (for ${raft.nodeId})").subscribe(raft.pipe.input)

    val input = raft.pipe.inputFor(peer.hostPort).dump(s"++++ TO $peer (from ${raft.nodeId})").map { msg =>
      import io.circe.syntax._
      val json = msg.asJson
      WebFrame.text(json.noSpaces)
    }

    input.subscribe(endpoint.toRemote)
  }
}
