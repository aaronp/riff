package riff.web.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import riff.monix.RaftMonix
import riff.raft.NodeId
import riff.raft.messages.RaftMessage
import riff.web.vertx.client.SocketClient
import streaming.api.sockets.WebFrame
import streaming.api.{Endpoint, HostPort}
import streaming.rest.{EndpointCoords, WebURI}

import scala.concurrent.duration.FiniteDuration

object Startup extends StrictLogging {


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
  def startServer(raft: RaftMonix[String], hostPort: HostPort, staticPath: Option[String])(implicit sched: Scheduler, socketTimeout: FiniteDuration, vertx: Vertx): ScalaVerticle = {
    val cluster = raft.cluster

    // try to connect to the other services
    val peerNames = cluster.peers.toSet

    Server.startSocket(hostPort, staticPath) {
      case HostPort(peer) =>
        (newClientWebsocketConnection: ServerEndpoint) => connectClientToPeer(raft, peer, newClientWebsocketConnection)
      case unknown =>
        (endpoint: ServerEndpoint) =>
          endpoint.fromRemote.consumeWith(Consumer.cancel)
          endpoint.toRemote.onError(new Exception(s"Invalid path '$unknown'. Expected one of ${peerNames.toList.sorted.mkString(",")}"))
    }
  }

  /**
    * When we start up, we try to connect a socket to each of our peers
    *
    * @param builder
    * @param sched
    * @param socketTimeout
    * @return a map of node IDs to clients
    */
  private def connectToPeers(builder: RaftMonix[String])(implicit socketTimeout: FiniteDuration, vertx: Vertx): Map[NodeId, SocketClient] = {
    import builder.scheduler

    val cluster = builder.cluster
    val name    = builder.nodeId
    val pears = cluster.peers.map { peerHostPortString =>
      val peerHostPort     = HostPort.unapply(peerHostPortString).getOrElse(sys.error(s"Couldn't parse peer as host:port: $peerHostPortString"))
      val endpoint = EndpointCoords(peerHostPort, WebURI.post(name))
      logger.info(s"Connecting to $peerHostPortString at $endpoint")
      val client = SocketClient.connect(endpoint, name) { endpoint => //
        connectClientToPeer(builder, peerHostPort, endpoint)
      }
      (peerHostPortString, client)
    }
    pears.toMap.ensuring(_.size == pears.size)
  }

  private def connectClientToPeer(raft: RaftMonix[String], peer: HostPort, endpoint: Endpoint[WebFrame, WebFrame]): Unit = {

    import raft.scheduler
    import riff.json.implicits._
    val fromMessages: Observable[RaftMessage[String]] = endpoint.fromRemote.dump(s"---- FROM ${peer} (for ${raft.nodeId})").flatMap { frame =>
      val json = frame.asText.getOrElse(sys.error("binary frames not supported"))
      import io.circe.parser._
      decode[RaftMessage[String]](json) match {
        case Left(err) =>
          logger.error(s"Couldn't unmarshall: $err:\n$json")
          Observable.empty
        case Right(msg) => Observable(msg)
      }
    }

    // subscribe our node to this stream
    fromMessages.subscribe(raft.pipe.input)

    val input = raft.pipe.inputFor(peer.hostPort).dump(s"++++ TO $peer (from ${raft.nodeId})").map { msg =>
      import io.circe.syntax._
      val json = msg.asJson
      WebFrame.text(json.noSpaces)
    }

    input.subscribe(endpoint.toRemote)
  }
}
