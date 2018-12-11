package riff.raft.vertx.server

import java.nio.file.Path

import eie.io.{FromBytes, ToBytes}
import io.vertx.scala.core.{Vertx, VertxOptions}
import monix.execution.Scheduler
import riff.monix.{MonixClock, RaftMonix}
import riff.raft.NodeId
import riff.raft.node.RaftCluster
import riff.raft.timer.RaftClock
import streaming.api.HostPort

import scala.concurrent.duration._
import scala.reflect.ClassTag

class VertxClusterConfig(val name: NodeId, //
                         val dataDir: Path, //
                         val hostPort: HostPort, //
                         val socketTimeout: FiniteDuration, //
                         val staticPath: Option[String], //
                         val clusterNodes: Set[HostPort],
                         val maxAppendSize: Int,
                         val createDirIfNotExists: Boolean)(implicit val scheduler: Scheduler, clock: RaftClock) { //

  def vertx(): Vertx = {
      val clusterSize = clusterNodes.size + 1
      Vertx.vertx(VertxOptions().setWorkerPoolSize(clusterSize).setEventLoopPoolSize(4).setInternalBlockingPoolSize(4))
  }

  def cluster: RaftCluster = {
    RaftCluster(clusterNodes.map(_.hostPort) - name)
  }

  def mkNode[A: ToBytes: FromBytes: ClassTag](): RaftMonix[A] = {
    RaftMonix[A](name, dataDir, cluster, maxAppendSize, createDirIfNotExists)
  }
}

object VertxClusterConfig {

  import eie.io._

  private val NodeR = "node([0-9])".r

  def apply(name: NodeId, //
            dataDir: Path, //
            hostPort: HostPort, //
            socketTimeout: FiniteDuration, //
            staticPath: Option[String], //
            clusterNodes: Set[HostPort],
            maxAppendSize: Int,
            createDirIfNotExists: Boolean)(implicit scheduler: Scheduler, clock: RaftClock) = {

    new VertxClusterConfig(
      name,
      dataDir,
      hostPort,
      socketTimeout,
      staticPath,
      clusterNodes,
    maxAppendSize,
      createDirIfNotExists
    )
  }

  def fromArgs(userArgs: Array[String])(implicit scheduler: Scheduler) = {
    implicit val clock = MonixClock()
    val staticPath: Option[String] = userArgs.collectFirst {
      case candidate if candidate.asPath.isDir => candidate
    }

    // TODO - this is just convenience for testing ATM
    val hostPortOpt = userArgs.collectFirst {
      case NodeR(nr) => HostPort.localhost(7000 + nr.toInt)
    }

    val clusterNodes = (1 to 2).map { i =>
      HostPort.localhost(7000 + i)
    }

    hostPortOpt.map { hostPort =>
      val dataDir = s"target/${hostPort.hostPort}/.data".asPath
      VertxClusterConfig(name = hostPort.hostPort,
        dataDir = dataDir,
        hostPort = hostPort,
        socketTimeout = 1.minute,
        staticPath = staticPath,
        clusterNodes = clusterNodes.toSet - hostPort,
        1000,
        createDirIfNotExists = true
      )
    }
  }
}
