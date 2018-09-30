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

  def apply[A](name: String)(implicit sched: Scheduler): NamedPipe[A, A] = {
    val (feed: Observer[A], sink: Observable[A]) = Pipe.publishToOne[A].unicast
    val s: Subscriber[A] = Subscriber(feed, sched)
    val input: Subscriber[A] = BufferedSubscriber(s, OverflowStrategy.BackPressure(10))
    new NamedPipe[A, A](name, input, sink)
  }
}

class Router[In, Out](val pipes: Map[String, NamedPipe[In, Out]]) {
  def apply(name: String): NamedPipe[In, Out] = pipes(name)
}

object Router {

  def apply[A](firstName: String, theRest: String*)(implicit sched: Scheduler): Router[A, A] = {
    apply(theRest.toSet + firstName)
  }

  def apply[A](names: Set[String])(implicit sched: Scheduler): Router[A, A] = {
    val map = names.map { name => //
      name -> NamedPipe[A](name)
    }.toMap
    new Router(map)
  }
}
