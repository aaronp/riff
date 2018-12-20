package riff.rest

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

case class StreamingConfig(config: Config) {
  val hostPort: HostPort         = HostPort(config.getString("host"), config.getInt("port"))
  val staticPath: Option[String] = Option(config.getString("staticPath")).filterNot(_.isEmpty)

  lazy val computeScheduler: SchedulerService = Scheduler.computation()
  lazy val ioScheduler: SchedulerService      = Scheduler.io()
}
