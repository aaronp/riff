package riff.web.vertx.server
import java.nio.file.Path

import com.typesafe.scalalogging.StrictLogging
import eie.io._
import io.vertx.scala.core.{Vertx, VertxOptions}
import monix.execution.schedulers.SchedulerService
import monix.reactive.Observable
import riff.monix.{Raft, MonixClock, RiffSchedulers}
import riff.raft.node.RaftCluster
import riff.raft.{AppendStatus, NodeId}
import riff.web.vertx.client.SocketClient
import streaming.api.HostPort

import scala.concurrent.duration._

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val running = args match {
      case Array(name) if clusterNamesForSize(5).contains(name)                      => Started(name, 5)
      case Array(name, sizeStr) if clusterNamesForSize(sizeStr.toInt).contains(name) => Started(name, sizeStr.toInt)
      case other                                                                     => sys.error(s"Usage: Expected the name and an optional cluster size (e.g. one of ${clusterNamesForSize(3)}, 3), but got '${other.mkString(" ")}'")
    }

    running.close()
    logger.info("Goodbye!")
  }

  def clusterNamesForSize(size: Int) = (1 to size).map(i => s"node$i").toSet

  def portForName(name: String) = 8000 + name.filter(_.isDigit).toInt

  case class Started(name: String, clusterSize: Int) extends AutoCloseable {
    implicit val scheduler: SchedulerService = RiffSchedulers.computation.scheduler
    implicit val socketTimeout               = 1.minute
    implicit val clock                       = MonixClock()
    val port                                 = portForName(name)
    val dataDir: Path                        = name.asPath.resolve("target/.data")
    val hostPort: HostPort                   = HostPort.localhost(port)
    val cluster: RaftCluster.Fixed           = RaftCluster(clusterNamesForSize(5) - name)
    implicit val vertx                       = Vertx.vertx(VertxOptions().setWorkerPoolSize(clusterSize + 1).setEventLoopPoolSize(4).setInternalBlockingPoolSize(4))

    val builder: Raft[String] = Raft[String](name, dataDir, cluster)

    // eagerly create the pipe
    builder.pipe
    logger.info(s"Connecting to peers...")
    val clients: Map[NodeId, SocketClient] = Startup.connectToPeers(builder)
    logger.info(s"Starting the server on ${hostPort} for $cluster")
    val verticle = Startup.startServer(builder, hostPort)
    logger.info(s"Handling messages on ${hostPort} for $cluster")

    builder.timerCallback.receiveTimeouts.foreach { _ => //
      logger.info(s"${name} got received HB timeout")
    }
    builder.timerCallback.sendTimeout.foreach { _ => //
      logger.info(s"${name} got send HB timeout")
    }
    builder.stateCallback.events.foreach { event => //
      logger.info(s"${name} noticed $event")
    }
    builder.log.appendResults().foreach { event => //
      logger.info(s"${name} log appended $event")
    }
    builder.log.committedEntries().foreach { event => //
      logger.info(s"${name} log committed $event")
    }

    builder.raftNode.resetReceiveHeartbeat()

    def client = builder.pipe.client
    override def close(): Unit = {
      builder.raftNode.cancelSendHeartbeat()
      builder.raftNode.cancelReceiveHeartbeat()
      scheduler.shutdown()
      verticle.stop()
      clients.values.foreach(_.stop())
    }
  }

}
