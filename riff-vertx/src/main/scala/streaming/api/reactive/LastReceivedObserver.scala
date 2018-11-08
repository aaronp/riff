package streaming.api.reactive

import monix.execution.Ack
import monix.reactive.Observer
import streaming.api.reactive.LastReceivedObserver.LastElementException

import scala.concurrent.Future

/**
  * An Observer which keeps track of the last element received, and sends that
  * element in a [[LastElementException]] in the case of error, giving downstream
  * subscriptions the chance to recover accordingly
  *
  * @param obs
  * @tparam T
  */
case class LastReceivedObserver[T](obs: Observer[T]) extends Observer[T] {
  protected var lastElem: Option[T] = None

  override def onNext(elem: T): Future[Ack] = {
    lastElem = Option(elem)
    obs.onNext(elem)
  }

  override def onError(ex: Throwable): Unit = {
    val newExp = lastElem.fold(ex)(new LastElementException[T](_, ex))
    obs.onError(newExp)
  }

  override def onComplete(): Unit = {
    obs.onComplete()
  }
}

object LastReceivedObserver {

  case class LastElementException[T](last: T, err: Throwable) extends Throwable

}
