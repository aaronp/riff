package riff.monix
import monix.execution.Scheduler

object RiffSchedulers {

  object computation {
    implicit val scheduler = Scheduler.computation()
  }

  object io {
    implicit val scheduler = Scheduler.io()
  }

}
