package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging
import eie.io.{FromBytes, ToBytes}
import io.circe.{Decoder, Encoder}
import io.vertx.scala.core.Vertx
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import monix.reactive.Observable
import riff.monix.RiffSchedulers
import riff.raft.{NodeId, RaftClient}
import riff.vertx.client.SocketClient

import scala.reflect.ClassTag

/**
  * Provides an entry-point for starting a vertx service which can connect a RaftNode
  */
object RunningVertxService extends StrictLogging {

  /**
  * Starts a Vertx web service
    * @param args the user agrs
    * @return a running vertx services
    */
  def start(args: Array[String]): Option[RunningVertxService[String]] = {
    implicit val scheduler = delegateScheduler
    VertxClusterConfig.fromArgs(args).map { config =>
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

      //        running.close()
      //        logger.info("Goodbye!")
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

}
case class RunningVertxService[A: ClassTag: ToBytes: FromBytes: Encoder: Decoder](config: VertxClusterConfig)(implicit val scheduler: Scheduler, vertx: Vertx)
    extends AutoCloseable with StrictLogging {

  implicit private val socketTimeout = config.socketTimeout
  val raft                           = config.mkNode[A]
  val cluster                        = config.cluster
  val hostPort                       = config.hostPort

  logger.info(s"Starting the server on ${hostPort} for $cluster")
  val verticle = Startup.startServer[A](raft, hostPort, config.staticPath)

  logger.info(s"Trying to connect to peers...")
  val clients: Map[NodeId, SocketClient] = Startup.connectToPeers(raft)

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
    raft.cancelHeartbeats()
    scheduler match {
      case ss: SchedulerService => ss.shutdown()
      case _                    =>
    }
    verticle.stop()
    clients.values.foreach(_.stop())
  }
}
