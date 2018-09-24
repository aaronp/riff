package riff.monix
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService

object RiffSchedulers {

  object computation {
    implicit val scheduler: SchedulerService = Scheduler.computation()
  }

  object io {
    implicit val scheduler = Scheduler.io()
  }

}
