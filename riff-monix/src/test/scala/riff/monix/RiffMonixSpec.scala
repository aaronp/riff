package riff.monix
import monix.execution.schedulers.ExecutorScheduler
import monix.execution.{ExecutionModel, Scheduler, UncaughtExceptionReporter}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.RiffThreadedSpec
import riff.raft.timer.RandomTimer
import concurrent.duration._

abstract class RiffMonixSpec extends RiffThreadedSpec with Eventually with BeforeAndAfterAll with LowPriorityRiffMonixImplicits {

  implicit def scheduler = ExecutorScheduler(execCtxt, UncaughtExceptionReporter.LogExceptionsToStandardErr, ExecutionModel.SynchronousExecution)

  override def afterAll(): Unit = {
    scheduler.shutdown()
  }

  def newClock(implicit s: Scheduler) = {
    MonixClock(10.millis, RandomTimer(50.millis, 100.millis))
  }
}
