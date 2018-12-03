package riff.raft.client
import riff.RiffSpec
import riff.raft.AppendStatus
import riff.raft.log._
import riff.raft.messages.AppendEntriesResponse

class SingleAppendFSMTest extends RiffSpec {

  val appendErrors = List(
    AttemptToAppendLogEntryAtEarlierTerm(LogCoords.Empty, LogCoords.Empty),
    AttemptToSkipLogEntry(LogCoords.Empty, 2),
    NotTheLeaderException("the node", 4, Option("Some new leader"))
  )
  "SingleAppendFSM" should {
    "ignore append errors until it receives its explicit first log append message" in {
      val fsm: SingleAppendFSM    = SingleAppendFSM("the node", 7)
      val result: SingleAppendFSM = fsm.update(StateUpdateMsg.logAppend(AttemptToSkipLogEntry(LogCoords.Empty, 2)))
      result shouldBe InitialState("the node", 7)
    }
    appendErrors.foreach { err =>
      s"move to an error state if the initial log append is ${err.getClass.getSimpleName}" in {
        SingleAppendFSM("the node", 7).update(StateUpdateMsg.initialAppend(err)) shouldBe ErrorState(err)
      }
    }
    appendErrors.foreach { err =>
      s"add an error if we encounter a ${err.getClass.getSimpleName} in the log" in {
        val fsm @ FilteringState(status, _) = SingleAppendFSM("the node", 7).update(StateUpdateMsg.initialAppend(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1))))
        val expected                        = status.copy(errorAfterAppend = Option(err))
        fsm.update(StateUpdateMsg.logAppend(err)) shouldBe EarlyTerminationOnErrorState(expected)
      }
    }
    "add a committed log coordinate if it was one of the appended coordinates" in {
      val fsm @ FilteringState(_, _)         = SingleAppendFSM("the node", 7).update(StateUpdateMsg.initialAppend(LogAppendSuccess(LogCoords(10, 100), LogCoords(10, 103))))
      val FilteringState(updated, Some(msg)) = fsm.update(StateUpdateMsg.logCommit(LogCoords(10, 100)))

      msg shouldBe AppendStatus(
        leaderAppendResult = LogAppendSuccess(LogCoords(term = 10, index = 100), LogCoords(term = 10, index = 103), List()),
        clusterSize = 7,
        appendedCoords = Set(LogCoords(term = 10, index = 100), LogCoords(term = 10, index = 101), LogCoords(term = 10, index = 102), LogCoords(term = 10, index = 103)),
        appended = Map("the node" -> AppendEntriesResponse(10, true, 103)),
        committed = Set(LogCoords(term = 10, index = 100)),
        errorAfterAppend = None
      )

      updated.committed shouldBe Set(LogCoords(10, 100))
    }
  }
}
