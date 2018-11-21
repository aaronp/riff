package riff.web.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.core.Vertx
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import riff.monix.Raft
import riff.raft.NodeId
import riff.raft.messages.RaftMessage
import riff.web.vertx.client.SocketClient
import riff.web.vertx.server.Main.portForName
import streaming.api.sockets.WebFrame
import streaming.api.{Endpoint, HostPort}
import streaming.rest.{EndpointCoords, WebURI}

import scala.concurrent.duration.FiniteDuration

object Startup extends StrictLogging {

  /**
    * When we start up, we try to connect a socket to each of our peers
    *
    * @param builder
    * @param sched
    * @param socketTimeout
    * @return
    */
  def connectToPeers(builder: Raft[String])(implicit socketTimeout: FiniteDuration, vertx: Vertx): Map[NodeId, SocketClient] = {
    import builder.scheduler

    val cluster = builder.raftNode.cluster
    val name    = builder.raftNode.nodeId
    val pears = cluster.peers.map { peerName =>
      val port     = portForName(peerName)
      val endpoint = EndpointCoords(HostPort.localhost(port), WebURI.post(name))
      logger.info(s"Connecting to $peerName at $endpoint")
      val client = SocketClient.connect(endpoint, name) { endpoint => //
        connectEndpoint(builder, peerName, endpoint)
      }
      (peerName, client)
    }
    pears.toMap.ensuring(_.size == pears.size)
  }

  def startServer(builder: Raft[String], hostPort: HostPort)(implicit sched: Scheduler, socketTimeout: FiniteDuration, vertx: Vertx): ScalaVerticle = {
    val cluster = builder.raftNode.cluster

    // try to connect to the other services

    val peerNames = cluster.peers.toSet
    object PeerName {
      def unapply(path: String) = {
        val scrubbed = path.filter(_.isLetterOrDigit)
        if (peerNames.contains(scrubbed)) {
          Some(scrubbed)
        } else {
          None
        }
      }
    }

    Server.start(hostPort) {
      case PeerName(name) =>
        (endpoint: ServerEndpoint) =>
          connectEndpoint(builder, name, endpoint)
      case unknown =>
        (endpoint: ServerEndpoint) =>
          endpoint.fromRemote.consumeWith(Consumer.cancel)
          endpoint.toRemote.onError(new Exception(s"Invalid path '$unknown'. Expected one of ${peerNames.toList.sorted.mkString(",")}"))
    }
  }

  def connectEndpoint(builder: Raft[String], peerName: String, endpoint: Endpoint[WebFrame, WebFrame]): Unit = {

    import builder.scheduler
    import riff.json.implicits._
    val fromMessages: Observable[RaftMessage[String]] = endpoint.fromRemote.flatMap { frame =>
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
    fromMessages.dump(s"from $peerName").subscribe(builder.pipe.input)

    val input = builder.pipe.inputFor(peerName).dump(s"to $peerName").map { msg =>
      import io.circe.syntax._
      val json = msg.asJson
      WebFrame.text(json.noSpaces)
    }

    input.subscribe(endpoint.toRemote)
  }
}
