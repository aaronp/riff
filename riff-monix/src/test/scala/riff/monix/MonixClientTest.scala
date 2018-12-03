package riff.monix
import monix.execution.Ack
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords, RaftLog}
import riff.raft.messages.{AppendData, AppendEntriesResponse, RaftMessage, ReceiveHeartbeatTimeout}
import riff.raft.timer.LoggedInvocationClock
import riff.raft.{AppendOccurredOnDisconnectedLeader, AppendStatus}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

class MonixClientTest extends RiffMonixSpec {

  "MonixClient.append" should {
    "complete w/ an error if the log observable ends up replacing the very coordinates we've appended after multiple append results are received" ignore {

      withScheduler { implicit scheduler =>
        val leaderLogResult: LogAppendSuccess = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 3))

        Given("Some a MonixClient based on the input to a raft node and some observable log coordinates")

        import riff.monix.log.ObservableLog._
        val obsLog = RaftLog.inMemory[String]().observable

        // we could use a monix Var or some other in-test version of Observable[LogAppendResult] here, but it's more of a
        // realistic test (more prod-like) to use the actual observable log which will drive it in practice
        val logResults: Observable[LogAppendResult] = obsLog.appendResults

        val clientUnderTest: MonixClient[String] = {
          // out fake Observer of the inputs to a RaftNode
          object TestNodeInput extends Observer[RaftMessage[String]] {
            // create the first append status as if it came from the leader
            val firstAppendResult: AppendStatus = AppendStatus(
              leaderAppendResult = leaderLogResult,
              appended = Map("leader node id" -> AppendEntriesResponse.ok(1, 1)),
              appendedCoords = leaderLogResult.appendedCoords,
              clusterSize = 5,
              false
            )
            val secondAppendResult = AppendStatus(
              leaderAppendResult = leaderLogResult,
              appended = Map("leader node id" -> AppendEntriesResponse.ok(1, 1), "follower node id" -> AppendEntriesResponse.ok(1, 1)),
              appendedCoords = leaderLogResult.appendedCoords,
              clusterSize = 5,
              false
            )
            override def onNext(elem: RaftMessage[String]): Future[Ack] = {
              elem match {
                case append @ AppendData(_, data) =>
                  data shouldBe Array("first", "second", "third")
                  val ss = append.statusSubscriber
                  ss.onNext(firstAppendResult)
                  ss.onNext(secondAppendResult)
              }
              Ack.Continue
            }
            override def onError(ex: Throwable): Unit = ???
            override def onComplete(): Unit           = ???
          }

          val logPublisher = logResults.filter(_ != null).replay
          logPublisher.connect()
          MonixClient[String](TestNodeInput, logPublisher)
        }

        And("some data is appended")
        val appendResult: Observable[AppendStatus] = clientUnderTest.append("first", "second", "third")

        And("The log append sends its first update")
        obsLog.append(LogCoords(1, 1), "first", "second", "third")

        //
        // this step is necessary to test as just electing a new leader isn't sufficient -- this leader, which was
        // disconnected, could STILL be re-elected and then replicate its uncommitted entries. It's only when an
        // old leader sees that it's uncommitted entries have been replaced by another leader's append requests that
        // we can be certain that we're in error and thus end its initial stream w/ an error
        //
        When("another log append message comes through which specifies one of the first entries was replaced")
        obsLog.append(LogCoords(2, 3), "new third")

        Then("The appendResult should finish in error")
        val err = intercept[AppendOccurredOnDisconnectedLeader] {
          //implicit val eq = Eq[AppendStatus](_ == _)
          val taken: List[AppendStatus] = appendResult.take(3).toListL.runSyncUnsafe(testTimeout)
          fail(s"Expected the replaced log entry to fail the stream, but got: ${taken}")
        }
        err.originalAppend shouldBe leaderLogResult
        err.newAppend shouldBe LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 3), replacedLogCoords = Seq(LogCoords(1, 3)))
      }
    }

    "send notifications when the append responses are received" in {

      withScheduler { implicit scheduler =>
        implicit val clock = new LoggedInvocationClock

        val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)

        try {
          fiveNodeCluster.values.head.input.onNext(ReceiveHeartbeatTimeout)

          val leader = eventually {
            fiveNodeCluster.values.find(_.handler.state().isLeader).get
          }

          var done     = false
          val received = ListBuffer[AppendStatus]()
          leader.client
            .append("XYZ123")
            .doOnComplete { () =>
              done = true
            }
            .foreach { x =>
              received += x
            }

          eventually {
            withClue(s"The append stream never completed after having received: ${received.mkString("; ")}") {
              done shouldBe true
            }
          }
        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
      }
    }
  }
}
