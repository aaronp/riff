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

/**
  * Represents the configurable parts of a Vertx web service
  *
  * @param name
  * @param dataDir
  * @param hostPort
  * @param socketTimeout
  * @param staticPath
  * @param clusterNodes
  * @param maxAppendSize
  * @param createDirIfNotExists
  * @param scheduler
  * @param clock
  */
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

/**
  * Provides functions for creating a configuration
  */
object VertxClusterConfig {

  import eie.io._

  private val StaticPathR = "staticPath=(.*)".r

  private val NodeR = "node([0-9])".r

  def apply(name: NodeId, //
            dataDir: Path, //
            hostPort: HostPort, //
            socketTimeout: FiniteDuration, //
            staticPath: Option[String], //
            clusterNodes: Set[HostPort],
            maxAppendSize: Int,
            createDirIfNotExists: Boolean)(implicit scheduler: Scheduler, clock: RaftClock): VertxClusterConfig = {

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

  /**
    * pareses the user-args into an optional [[VertxClusterConfig]] if the user args are valid
    *
    * This signature should be relatively stable, though at the moment the implementation is a bit noddy...
    * it just expects 'node1' or 'node2' and uses the integer part of the name as the port
    *
    * @param userArgs the user args. If an argument is a directory or matches staticPath={dir}, then that directory will be used to serve static resources
    * @param portOffset the port number to add to the 'N' where the nodes are specified as nodeN (e.g. node1, node2, etc)
    * @param scheduler a monix scheduler
    * @return an optionally parsed config if the args are valid
    */
  def fromArgs(userArgs: Array[String], portOffset: Int)(implicit scheduler: Scheduler): Option[VertxClusterConfig] = {
    implicit val clock = MonixClock()
    val staticPath: Option[String] = userArgs.collectFirst {
      case StaticPathR(dir)                    => dir
      case candidate if candidate.asPath.isDir => candidate
    }

    // TODO - this is just convenience for testing ATM
    val hostPortOpt = userArgs.collectFirst {
      case NodeR(nr) => HostPort.localhost(portOffset + nr.toInt)
    }

    val clusterNodes = (1 to 2).map { i =>
      HostPort.localhost(portOffset + i)
    }

    hostPortOpt.map { hostPort =>
      val dataDir = s"target/${hostPort.hostPort}/.data".asPath
      VertxClusterConfig(
        name = hostPort.hostPort,
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
