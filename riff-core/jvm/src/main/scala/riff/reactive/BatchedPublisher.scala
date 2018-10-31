package riff.reactive
import org.reactivestreams.{Publisher, Subscriber}

case class BatchedPublisher[A](underlying: Publisher[A], val minimumRequestedThreshold: Int, val subscriptionBatchSize: Int) extends Publisher[A] {
  override def subscribe(s: Subscriber[_ >: A]): Unit = {
    underlying.subscribe(BatchedSubscriber[A](s, minimumRequestedThreshold, subscriptionBatchSize))
  }
}
