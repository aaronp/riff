package riff.reactive
import org.reactivestreams.{Publisher, Subscriber}

/**
  * Represents the ability for some type F[_] to be converted to a publisher, in addition to perform some basic algebra on
  * the F[_] (such as collecting into another F[_])
  *
  * @tparam F the typeclass which can be represented as a publisher
  */
trait AsPublisher[F[_]] {
  def asPublisher[A](f: F[A]): Publisher[A]

  /** apply the partial function to the F[A], which is the equivalent of a filter/map operation
    *
    * @param f the F[A] instance to wrap
    * @param func the collection function
    * @tparam A the original type
    * @tparam B the mapped (collected) type
    * @return a new F[B] based on the application of the func to the underlying F[A]
    */
  def collect[A, B](f: F[A])(func: PartialFunction[A, B]): F[B]

  def map[A, B](f: F[A])(func: A => B): F[B]

  /**
    * Convenience method for 'subscribe', but which returns the subscriber instance:
    * {{{
    *   val mySub = publisher.subscribeWith(new FooSubscriber) // mySub will be a FooSubscriber
    * }}}
    *
    * @param s the subscriber to return
    * @tparam S the subscriber type
    * @return the same subscriber
    */
  final def subscribeWith[A, S <: Subscriber[_ >: A]](f: F[A], s: S): S = {
    asPublisher(f).subscribe(s)
    s
  }
}

object AsPublisher {
  implicit object Identity extends AsPublisher[Publisher] {
    override def asPublisher[A](f: Publisher[A]): Publisher[A] = f
    override def collect[A, B](f: Publisher[A])(func: PartialFunction[A, B]): Publisher[B] = CollectPublisher(f)(func)
    override def map[A, B](f: Publisher[A])(func: A => B): Publisher[B] = MapPublisher(f)(func)
  }

  def apply[F[_]](implicit instance: AsPublisher[F]): AsPublisher[F] = instance

  object syntax {
    implicit class RichPub[A, F[A]](val fa: F[A]) extends AnyVal {
      def asPublisher(implicit ev: AsPublisher[F]): Publisher[A] = ev.asPublisher(fa)

      def subscribeWith[S <: Subscriber[A]](s: S)(implicit ev: AsPublisher[F]): S = {
        asPublisher.subscribe(s)
        s
      }

      def collect[B](func: PartialFunction[A, B])(implicit ev: AsPublisher[F]): F[B] = {
        AsPublisher[F].collect[A, B](fa)(func)
      }

      def map[B](func: A => B)(implicit ev: AsPublisher[F]): F[B] = {
        AsPublisher[F].map[A, B](fa)(func)
      }
    }
  }
}
