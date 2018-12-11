package riff.web.vertx.server
import com.typesafe.scalalogging.StrictLogging
import io.vertx.scala.core.{Vertx, VertxOptions}
import monix.execution.schedulers.SchedulerService
import monix.reactive.Observable
import riff.monix.RiffSchedulers
import riff.raft.{NodeId, RaftClient}
import riff.web.vertx.client.SocketClient

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {
    implicit val scheduler = new RiffSchedulers.Delegate(RiffSchedulers.computation.newScheduler()) {
      override def shutdown(): Unit = {
        logger.warn("shutting down scheduler")
        super.shutdown()
      }
    }

    VertxClusterConfig.fromArgs(args) match {
      case None =>
        sys.error(s"Usage: Expected the name and an optional cluster size but got '${args.mkString(" ")}'")
      case Some(config) =>
        val running: Started = Started(config)

        logger.info(s"Started ${running.config.name}")
//        running.close()
//        logger.info("Goodbye!")
    }
  }

  case class Started(config: VertxClusterConfig) extends AutoCloseable {
    implicit val scheduler: SchedulerService = {
      new RiffSchedulers.Delegate(RiffSchedulers.computation.newScheduler()) {
        override def shutdown(): Unit = {
          logger.warn("Shutting down the scheduler")
          super.shutdown()
        }
      }
    }

    implicit val vertx = {
      val clusterSize = config.clusterNodes.size + 1
      Vertx.vertx(VertxOptions().setWorkerPoolSize(clusterSize).setEventLoopPoolSize(4).setInternalBlockingPoolSize(4))
    }

    val raft     = config.mkNode[String]
    val cluster  = config.cluster
    val hostPort = config.hostPort
    logger.info(s"Trying to connect to peers...")
    val clients: Map[NodeId, SocketClient] = Startup.connectToPeers(raft)(config.socketTimeout, vertx)
    logger.info(s"Starting the server on ${hostPort} for $cluster")
    val verticle = Startup.startServer(raft, hostPort, config.staticPath)(config.scheduler, config.socketTimeout, vertx)
    logger.info(s"Handling messages on ${hostPort} for $cluster")

    // this isn't JUST debug -- with zero subscriptions the node doesn't do anything.
    // we need at least one subscriber to actually make the node do work
    raft.pipe.output.foreach { res =>
      logger.info(s"${raft.nodeId} sending $res")
    }
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

    raft.resetReceiveHeartbeat()

    def client: RaftClient[Observable, String] = raft.pipe.client

    override def close(): Unit = {
      raft.cancelHeartbeats()
      scheduler.shutdown()
      verticle.stop()
      clients.values.foreach(_.stop())
    }
  }

}
