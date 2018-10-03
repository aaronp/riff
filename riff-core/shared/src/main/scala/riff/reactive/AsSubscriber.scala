package riff.reactive
import org.reactivestreams.Subscriber

/**
  * Represents any F[_] which can be represented as a Subscriber
  *
  * @tparam F
  */
trait AsSubscriber[F[_]] {
  def asSubscriber[A](f: F[A]): Subscriber[A]
}

object AsSubscriber {

  implicit object Identity extends AsSubscriber[Subscriber] {
    override def asSubscriber[A](f: Subscriber[A]): Subscriber[A] = f
  }

  def apply[F[_]](implicit instance: AsSubscriber[F]): AsSubscriber[F] = instance

  object syntax {
    implicit class RichPub[A, F[A]](val fa: F[A]) extends AnyVal {
      def asSubscriber(implicit ev: AsSubscriber[F]): Subscriber[A] = ev.asSubscriber(fa)
    }
  }
}
