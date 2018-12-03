package riff.monix
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords}
import riff.raft.messages._
import riff.raft.{AppendStatus, NodeId}

import scala.collection.mutable.ListBuffer

class AppendStateTest extends RiffMonixSpec {

  "AppendState.prepareAppendFeed" should {

    "publish updates when either the nodeInput or appendResult change" in {
      withScheduler { implicit s =>
        val inputToNode   = Var[RaftMessage[String]](ReceiveHeartbeatTimeout)
        val logFeed       = Var[LogAppendResult](null)
        val committedFeed = Var[LogCoords](null)

        val statusFeed: (Observer[LogAppendResult], Observable[AppendStatus]) = AppendState.prepareAppendFeed(
          "the node id",
          7,
          inputToNode,
          logFeed.filter(_ != null),
          committedFeed.filter(_ != null),
          10
        )

        val received                = ListBuffer[AppendStatus]()
        val (appendResultObj, feed) = statusFeed
        feed.foreach { next =>
          println(next)
          received += next
        }
      }
    }
    "complete in error when a LogAppendResult in error is sent" in {
      ???
    }
    "be reusable after a LogAppendResult in error is sent" in {
      ???
    }
    "publish all events sent by the nodeInput prior to subcription" in {
      ???
    }
    "publish all events sent by the appendResults prior to subcription" in {
      ???
    }
  }

  "AppendState.combine" should {
    "supply events received before it was subscribed to" in {
      withScheduler { implicit s =>
        val inputToNode                                                              = Var[RaftMessage[String]](ReceiveHeartbeatTimeout)
        val logFeed                                                                  = Var[LogAppendResult](null)
        val combined: Observable[(LogAppendResult, (NodeId, AppendEntriesResponse))] = AppendState.combine[String](inputToNode, logFeed.filter(_ != null), 10)

        // send an event prior to subscribing
        inputToNode := AddressedMessage("from", AppendEntriesResponse.ok(1, 2))

        // and a log event
        logFeed := LogAppendSuccess(LogCoords.Empty, LogCoords.Empty)

        //... actually subscribe
        val received = ListBuffer[(LogAppendResult, (NodeId, AppendEntriesResponse))]()
        combined.foreach { next =>
          println(next)
          received += next
        }

        eventually {
          received should contain only (LogAppendSuccess(LogCoords.Empty, LogCoords.Empty) -> ("from", AppendEntriesResponse(1, true, 2)))
        }
      }
    }
  }
}
