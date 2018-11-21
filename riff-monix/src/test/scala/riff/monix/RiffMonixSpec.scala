package riff.monix
import java.util.concurrent.TimeUnit

import monix.execution.schedulers.{ExecutorScheduler, TrampolineExecutionContext}
import monix.execution.{ExecutionModel, Scheduler, UncaughtExceptionReporter}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.RiffThreadedSpec
import riff.raft.timer.RandomTimer

import scala.concurrent.duration._

abstract class RiffMonixSpec extends RiffThreadedSpec with Eventually with BeforeAndAfterAll with LowPriorityRiffMonixImplicits {

  /**
    * How long to wait for something NOT to happen - use sparingly!
    *
    * @return some amount of time to wait before ensuring e.g. no messages are received
    */
  def testNegativeTimeout: FiniteDuration = 100.millis

  def withScheduler[T](f: Scheduler => T): T = {

    val retVal = withExecCtxt { implicit execCtxt =>
      // some tests explicitly throw exceptions, so we should just write these down
      val reporter                             = UncaughtExceptionReporter.LogExceptionsToStandardErr
      val executorScheduler: ExecutorScheduler = ExecutorScheduler(execCtxt, reporter, ExecutionModel.SynchronousExecution)
      try {
        f(executorScheduler)
      } finally {
        executorScheduler.shutdown()
        executorScheduler.awaitTermination(testTimeout.toMillis, TimeUnit.MILLISECONDS, TrampolineExecutionContext.immediate).futureValue
      }
    }

    retVal
  }

  def newClock(implicit s: Scheduler): MonixClock = {
    MonixClock(100.millis, RandomTimer(300.millis, 500.millis))(s)
  }
}
