package riff.raft.reactive

import org.reactivestreams.Publisher
import riff.raft.log.{LogAppendResult, LogAppendSuccess}
import riff.raft.messages.{AddressedMessage, AppendData, AppendEntriesResponse, RaftMessage}
import riff.raft.node.{AddressedRequest, RaftMessageHandler, RaftNode}
import riff.raft.{AppendStatus, NodeId, NotTheLeaderException}
import riff.reactive.{AsPublisher, Publishers, ReplayPublisher, SingleSubscriber}

import scala.concurrent.ExecutionContext

/**
  *
  * The idea is to create a 'pipe' of RaftMessage[A] --> RaftNodeResult[A].
  *
  * We start with a simple pipe of RaftMessage[A] --> RaftMessage[A] where the input (subscriber)
  * is a [[riff.reactive.MultiSubscriberProcessor]] that can subscribe to the other nodes in the cluster,
  * as well as a [[riff.reactive.ReactiveTimerCallback]] (or similar) which publishes an event when a [[riff.raft.timer.RaftClock]]
  * times out.
  *
  * We can then map the 'output' Publisher to transform the inputs via this 'onMessage' handler, which intercepts [[AppendData]]
  * messages in order to fulfill their subscribers
  *
  * @param node the wrapped node which will handle the
  * @param input
  * @param ev$1
  * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
  * @tparam F
  */
class ReactiveHandler[A, F[_]: AsPublisher](node: RaftNode[A], input: F[RaftMessage[A]])(
  implicit executionContext: ExecutionContext)
    extends RaftMessageHandler[A] {

  override def onMessage(msg: RaftMessage[A]): Result = {
    input match {
      case append: AppendData[A, _] => //(subscriber, values) =>

        val appendResult = node.appendIfLeader(append.values)
        val statusPublisher: Publisher[_ <: AppendStatus] = handleAppendResult(appendResult)

        statusPublisher.subscribe(append.statusSubscriber)

        RaftNode.appendResponseAsRaftNodeResult(appendResult)
      case _ => node.onMessage(msg)
    }
  }

  private def handleAppendResult(appendResult: Either[NotTheLeaderException, (LogAppendResult, AddressedRequest[A])])
    : Publisher[_ <: AppendStatus] = {
    appendResult match {
      case Left(err) => Publishers.InError(err)
      case Right((err: Exception, _)) => Publishers.InError(err)
      case Right((success: LogAppendSuccess, requests)) =>
        val clusterSize = requests.requests.size + 1
        val committed = clusterSize == 1

        if (committed) {
          Publishers.Fixed(AppendStatus(success, Map(nodeId -> true), committed, clusterSize) :: Nil)
        } else {

          //
          // FYI, this looks a bit prettier when using a proper reactive streams lib (monix, fs2, etc)
          // as it adds 'fold', etc, which I've not bothered with here
          //

          val OneOffSubscriber = new ReplayPublisher[AppendStatus] with SingleSubscriber[RaftMessage[A]] {
            override implicit def ctxt: ExecutionContext = executionContext

            var status = AppendStatus(success, Map(nodeId -> true), false, clusterSize)
            override protected def doOnError(err: Throwable): Unit = {
              enqueueError(err)
            }
            override protected def doOnComplete(): Unit = {
              enqueueComplete()
            }
            override def doOnNext(msg: RaftMessage[A]): Unit = {
              msg match {
                case AddressedMessage(from, resp: AppendEntriesResponse) =>
                  if (success.contains(resp)) {
                    status = status.withResult(from, resp.success)

                    enqueueMessage(status)

                    if (status.appended.size == clusterSize) {
                      cancel()
                      enqueueComplete()
                    }
                  }
                case _ =>
              }
            }

            override protected val maxQueueSize: Int = 1000
            override protected val subscriptionBatchSize: Int = 100
            override protected val minimumRequestedThreshold: Int = 10
          }

          import AsPublisher.syntax._
          input.subscribeWith(OneOffSubscriber)

          OneOffSubscriber
        }
    }
  }

  override def nodeId: NodeId = node.nodeId
}
