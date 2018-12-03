package riff.monix
import monix.execution.{Cancelable, ExecutionModel, Scheduler}
import monix.execution.schedulers.SchedulerService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.TimeUnit

object RiffSchedulers {

  object computation {
    private val inst = Scheduler.computation()
    private def c: SchedulerService = {
      require(!inst.isTerminated, "TERMINATED!!!!!")
      require(!inst.isShutdown, "SHUT DOWN!!!!!")
      inst
    }
    implicit def scheduler: SchedulerService = new SchedulerService {
      override def isShutdown: Boolean   = c.isShutdown
      override def isTerminated: Boolean = c.isTerminated
      override def shutdown(): Unit = {
        println(s"""
                   |
                   |
                   |
                   |
                   | SHUTTING DOWN THE GLOBAL SCHEDULER!
                   |
                   |
                   |
                   |
           """.stripMargin)
        c.shutdown()
      }
      override def awaitTermination(timeout: Long, unit: TimeUnit, awaitOn: ExecutionContext): Future[Boolean] = {
        c.awaitTermination(timeout, unit, awaitOn)
      }
      override def withExecutionModel(em: ExecutionModel): SchedulerService = {
        c.withExecutionModel(em)
      }
      override def execute(command: Runnable): Unit = {
        c.execute(command)
      }
      override def reportFailure(t: Throwable): Unit = {
        c.reportFailure(t)
      }
      override def scheduleOnce(initialDelay: Long, unit: TimeUnit, r: Runnable): Cancelable = {
        c.scheduleOnce(initialDelay, unit, r)
      }
      override def scheduleWithFixedDelay(initialDelay: Long, delay: Long, unit: TimeUnit, r: Runnable): Cancelable = {
        c.scheduleWithFixedDelay(initialDelay, delay, unit, r)
      }
      override def scheduleAtFixedRate(initialDelay: Long, period: Long, unit: TimeUnit, r: Runnable): Cancelable = {
        c.scheduleAtFixedRate(initialDelay, period, unit, r)
      }
      override def clockRealTime(unit: TimeUnit): Long = {
        c.clockRealTime(unit)
      }
      override def clockMonotonic(unit: TimeUnit): Long = {
        c.clockMonotonic(unit)
      }
      override def executionModel: ExecutionModel = c.executionModel
    }
  }

}
