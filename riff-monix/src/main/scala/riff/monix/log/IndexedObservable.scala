package riff.monix.log
import monix.execution.{Ack, Scheduler}
import monix.reactive.observables.ConnectableObservable
import monix.reactive.subjects.PublishToOneSubject
import monix.reactive.{Observable, Observer, Pipe}

import scala.concurrent.Future

/**
  * Represents something which can be subscribed to from a particular index.
  *
  * You can subscribe this to some data stream, and it'll stick an index on each entry.
  *
  * @tparam A the observer type
  */
trait IndexedObservable[A] extends Observer[A] {

  /**
    * Allow something to subscribe from an arbitrary index.
    *
    * The trick is that the data coming through is a moving target.
    * What we want is:
    *
    * {{{
    *   historicData : Observable[(Long, A)] = ... //subscribeFromIndex to currentIndex
    *   futureData : Observable[(Long, A)] = ... // currentIndex until complete
    *
    *   return historicData ++ futureData
    * }}}
    *
    * It seems we want to do something like:
    * 1) create a conn
    *
    * @param subscribeFromIndex the index from which we want to subscribe
    * @return all the data (no gabs) from the subscribeFromIndex
    */
  def fromIndex(subscribeFromIndex: Int): Observable[(Long, A)]

  /** @return an observable which will publish the values from moment of subscription
    */
  def latest(): Observable[(Long, A)]
}

object IndexedObservable {

  def apply[A](lookUp: Long => A, firstIndex: Long = 0)(implicit scheduler: Scheduler) = {
    new Impl(lookUp, firstIndex)
  }

  class Impl[A](lookUp: Long => A, firstIndex: Long)(implicit scheduler: Scheduler) extends IndexedObservable[A] {

    @volatile private var counter = firstIndex
    private val pipe = Pipe.publish[(Long, A)]
    private val (sink, feed: Observable[(Long, A)]) = pipe.multicast

    override def latest(): Observable[(Long, A)] = feed

    /**
      * Allow something to subscribe from an arbitrary index.
      *
      * The trick is that the data coming through is a moving target.
      * What we want is:
      *
      * {{{
      *   historicData : Observable[(Long, A)] = ... //subscribeFromIndex to currentIndex
      *   futureData : Observable[(Long, A)] = ... // currentIndex until complete
      *
      *   return historicData ++ futureData
      * }}}
      *
      * It seems we want to do something like:
      * 1) create a conn
      *
      * @param subscribeFromIndex the index from which we want to subscribe
      * @return all the data (no gabs) from the subscribeFromIndex
      */
    def fromIndex(subscribeFromIndex: Int): Observable[(Long, A)] = {
      val subject = PublishToOneSubject[(Long, A)]
      val c: ConnectableObservable[(Long, A)] = ConnectableObservable.cacheUntilConnect(feed, subject)

      // at this point we'll have all the data from 'counter'
      c.connect()

      // our connectable observable *should* include all the data from the current value of counter, if not
      // before (as there seems to be race condition between setting up/connecting the observable and getting the historic elements)
      val indexFromWhichWeSwap = counter

      val exactlyAfterIndex = c.filter(_._1 > indexFromWhichWeSwap)

      val historicData = dataFrom(subscribeFromIndex, indexFromWhichWeSwap)
      historicData ++ exactlyAfterIndex
    }

    private def dataFrom(from: Long, to: Long): Observable[(Long, A)] = {
      val obs: Observable[Long] = Observable.fromIterable(from to to)
      obs.map(idx => (idx, lookUp(idx)))
    }

    override def onNext(elem: A): Future[Ack] = {
      val future = sink.onNext(counter -> elem)
      counter = counter + 1
      future
    }

    override def onError(ex: Throwable): Unit = {
      sink.onError(ex)
    }
    override def onComplete(): Unit = {
      sink.onComplete()
    }
  }

}
