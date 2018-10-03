package riff.reactive

import scala.collection.mutable.ListBuffer

class TestListener[A](override val minimumRequestedThreshold: Int = 0, override val subscriptionBatchSize: Int = 0)
    extends SingleSubscriber[A] {
  val received = ListBuffer[A]()
  val errors = ListBuffer[Throwable]()
  var completed = false
  override def doOnNext(t: A): Unit = {
    received += t
  }
  override def doOnError(t: Throwable): Unit = {
    errors += t
  }
  override def doOnComplete(): Unit = {
    completed = true
  }
}
