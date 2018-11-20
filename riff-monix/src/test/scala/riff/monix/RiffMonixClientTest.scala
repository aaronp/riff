package riff.monix
import monix.execution.Ack
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.raft.log.{LogAppendResult, LogAppendSuccess, LogCoords}
import riff.raft.messages.{AppendData, AppendEntriesResponse, RaftMessage}
import riff.raft.{AppendOccurredOnDisconnectedLeader, AppendStatus}

import scala.concurrent.Future

class RiffMonixClientTest extends RiffMonixSpec {

  "RiffMonixClient" should {
    "complete w/ an error if the log observable ends up replacing the very coordinates we've appended after multiple append results are received" in {

      val leaderLogResult: LogAppendSuccess = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 3))
      // create the first append status as if it came from the leader
      val firstAppendResult: AppendStatus = AppendStatus(
        leaderAppendResult = leaderLogResult,
        appended = Map("leader node id" -> AppendEntriesResponse.ok(1, 1)),
        appendedCoords = leaderLogResult.appendedCoords,
        clusterSize = 5
      )
      val secondAppendResult = AppendStatus(
        leaderAppendResult = leaderLogResult,
        appended = Map("leader node id" -> AppendEntriesResponse.ok(1, 1), "follower node id" -> AppendEntriesResponse.ok(1, 1)),
        appendedCoords = leaderLogResult.appendedCoords,
        clusterSize = 5
      )
      verifyError(leaderLogResult, firstAppendResult, secondAppendResult)
    }

  }

  def verifyError(leaderLogResult: LogAppendSuccess, appendStatusMessagesToReceive: AppendStatus*) = {

    Given("Some a RiffMonixClient based on the input to a raft node and some observable log coordinates")
    val logResults = Var[LogAppendResult](null)

    val clientUnderTest: RiffMonixClient[String] = {
      object TestNodeInput extends Observer[RaftMessage[String]] {
        override def onNext(elem: RaftMessage[String]): Future[Ack] = {
          elem match {
            case append @ AppendData(_, data) =>
              data shouldBe Array("first", "second", "third")
              val ss = append.statusSubscriber
              appendStatusMessagesToReceive.foreach(ss.onNext)
          }
          Ack.Continue
        }
        override def onError(ex: Throwable): Unit = ???
        override def onComplete(): Unit = ???
      }

      val logPublisher = logResults.filter(_ != null).replay
      logPublisher.connect()
      RiffMonixClient[String](TestNodeInput, logPublisher)
    }

    And("some data is appended")
    val appendResult: Observable[AppendStatus] = clientUnderTest.append("first", "second", "third")

    And("The log append sends its first update")
    logResults := leaderLogResult

    When("another log append message comes through which specifies one of the first entries was replaced")
    val subsequentReplaceResult = LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 5), replacedLogCoords = Seq(LogCoords(1, 3)))
    logResults := subsequentReplaceResult

    Then("The appendResult should finish in error")
    val err = intercept[AppendOccurredOnDisconnectedLeader] {
      appendResult.take(2).toListL.runSyncUnsafe(testTimeout)
    }
    err.originalAppend shouldBe leaderLogResult
    err.newAppend shouldBe subsequentReplaceResult
  }
}
