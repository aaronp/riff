package riff.monix
import monix.execution.Scheduler
import monix.reactive.{Observable, Observer, Pipe}

import scala.collection.mutable

/** @param name the name of this pipe
  * @param sink
  * @param source
  * @tparam A
  */
class NamedPipe[A](name: String, input: Observer[A]) {
  private val outputByName = mutable.HashMap[String, Observable[A]]()

  def outputFor(target: String, default: => Observable[A]): Observable[A] = {
    outputByName.getOrElseUpdate(target, default)
  }
}

object Router {

  /**
    * If we have 3 nodes A, B and C, then we should be able to subscribe the input each to the output of another.
    *
    *
    * @param name
    * @param theRest
    * @param sched
    * @tparam A
    */
  def forNames[A](name: String, theRest: String*)(implicit sched: Scheduler) = {
    val names = theRest.toSet + name

    val (sink: Observer[A], source: Observable[A]) = Pipe.publish[A].multicast

  }

  def forNames[A](names: Set[String])(newPipe: () => (Observer[A], Observable[A])) = {}
}

class Router[A](names: Set[String], newPipe: () => (Observer[A], Observable[A])) {}
