package riff.monix
import monix.execution.Scheduler
import monix.reactive.observers.{BufferedSubscriber, Subscriber}
import monix.reactive.{Observable, Observer, OverflowStrategy, Pipe}

/**
  *
  * @param name
  * @param bufferedSubscriber the input to this pipe, which can be subscribed to multiple sources
  * @param output an output
  * @tparam A
  */
class NamedPipe[In, Out](val name: String, val bufferedSubscriber: Subscriber[In], val output: Observable[Out])

object NamedPipe {

  def apply[A](name: String, bufferPolicy: OverflowStrategy[A] = OverflowStrategy.BackPressure(100))(
    implicit sched: Scheduler): NamedPipe[A, A] = {
    val (feed: Observer[A], sink: Observable[A]) = Pipe.publish[A].unicast
    val s: Subscriber[A] = Subscriber(feed, sched)
    new NamedPipe[A, A](name, BufferedSubscriber(s, bufferPolicy), sink)
  }
}

class Router[In, Out](val pipes: Map[String, NamedPipe[In, Out]]) {
  def apply(name: String): NamedPipe[In, Out] = pipes(name)
}

object Router {

  def apply[A](firstName: String, theRest: String*)(implicit sched: Scheduler): Router[A, A] = {
    apply(theRest.toSet + firstName)
  }

  def apply[A](names: Set[String]): Router[A, A] = {
    val map = names.map { name => //

      implicit val sched = Scheduler.computation(1, s"node $name")
      name -> NamedPipe[A](name)
    }.toMap
    new Router(map)
  }
}
