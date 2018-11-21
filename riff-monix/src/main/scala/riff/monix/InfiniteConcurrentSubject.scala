package riff.monix
import monix.execution.{Ack, Scheduler}
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Observable, Observer}

import scala.concurrent.Future

class InfiniteConcurrentSubject[A](private val subject: ConcurrentSubject[A, A])(implicit sched: Scheduler) {
  def output: Observable[A] = subject

  val input = new Observer[A] {
    override def onNext(elem: A): Future[Ack] = {
      subject.onNext(elem)
    }
    override def onError(ex: Throwable): Unit = {
      sched.reportFailure(ex)
    }
    override def onComplete(): Unit = {
      // ignore complete
    }
  }
}

object InfiniteConcurrentSubject {

  def apply[A](implicit scheduler: Scheduler): InfiniteConcurrentSubject[A] = {
    apply(ConcurrentSubject.publish[A])
  }

  def apply[A](subject: ConcurrentSubject[A, A])(implicit scheduler: Scheduler): InfiniteConcurrentSubject[A] = {
    new InfiniteConcurrentSubject(subject)
  }
}
