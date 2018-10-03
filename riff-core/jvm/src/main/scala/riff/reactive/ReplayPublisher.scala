package riff.reactive

/**
  * A publisher which will replay all elements from before any subscription begins
  *
  * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
  */
trait ReplayPublisher[A] extends AsyncPublisher[A] {

  private var values = List[A]()

  override final protected def enqueueMessages(messages: Iterable[A]): Unit = {
    values = messages.foldRight(values)(_ +: _)
    super.enqueueMessages(messages)
  }

  override final protected def enqueueMessage(message: A): Unit = {
    values = message +: values
    super.enqueueMessage(message)
  }

  /**
    * This is called before 'onSubscribe' is called
    * @param sub the subscription to add
    * @return the subscription
    */
  override protected def add(sub: AsyncSubscription[A]): AsyncSubscription[A] = {
    val retVal = super.add(sub)

    // at this point we can charge this subscription w/ all the values already received
    sub.enqueueAll(values.reverse)

    retVal
  }
}
