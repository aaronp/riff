package riff.monix
import monix.execution.Scheduler

object RiffSchedulers {

  implicit val DefaultScheduler = Scheduler.computation()

}
