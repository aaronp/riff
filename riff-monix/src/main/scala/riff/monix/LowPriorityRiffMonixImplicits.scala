package riff.monix
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer}
import org.reactivestreams.{Publisher, Subscriber}
import riff.reactive.{AsPublisher, AsSubscriber}

object LowPriorityRiffMonixImplicits extends LowPriorityRiffMonixImplicits

trait LowPriorityRiffMonixImplicits {

  implicit def observerAsSubscriber(implicit scheduler: Scheduler) = new AsSubscriber[Observer] {
    override def asSubscriber[A](f: Observer[A]): Subscriber[A] = f.toReactive
  }

  implicit def observableAsPublisher(implicit scheduler: Scheduler) = new AsPublisher[Observable] {
    override def asPublisher[A](f: Observable[A]): Publisher[A] = {
      f.toReactivePublisher(scheduler)
    }
    override def collect[A, B](f: Observable[A])(func: PartialFunction[A, B]): Observable[B] = {
      f.collect(func)
    }
    override def map[A, B](f: Observable[A])(func: A => B): Observable[B] = {
      f.map(func)
    }
    override def cons[A](value: A, publisher: Observable[A]): Observable[A] = {
      value +: publisher
    }
    override def takeWhile[A](publisher: Observable[A])(predicate: A => Boolean): Observable[A] = {
      publisher.takeWhile(predicate)
    }
    override def takeWhileIncludeLast[A](publisher: Observable[A])(predicate: A => Boolean): Observable[A] = {
      val options: Observable[Option[A]] = publisher.flatMap {
        case next if predicate(next) => Observable(Option(next))
        case next                    => Observable(Option(next), None)
      }
      options.takeWhile(_.isDefined).map(_.get)
    }
  }

}
