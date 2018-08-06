package riff.raft.election
import riff.RiffSpec
import riff.raft.log.LogCoords
class HasInfoForVoteTest extends RiffSpec {
  import HasInfoForVote.ops._

  "HasInfoForVote.replyTo" should {
    "not grant a vote if it is for an earlier term" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(0, 0))(_ => false)
      "some raft node".replyTo(RequestVoteData(term = ourTerm - 1, lastLogIndex = 0, lastLogTerm = 0)) shouldBe RequestVoteReply(ourTerm, false)
    }
    "not grant a vote if we already cast a vote in the term" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(0, 0))(_ => true)
      "some raft node".replyTo(RequestVoteData(term = ourTerm + 1, lastLogIndex = 0, lastLogTerm = 0)) shouldBe RequestVoteReply(ourTerm, false)
    }
    "not grant a vote if it is for the same term" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(0, 0))(_ => false)
      "some raft node".replyTo(RequestVoteData(term = ourTerm, lastLogIndex = 0, lastLogTerm = 0)) shouldBe RequestVoteReply(ourTerm, false)
    }
    "not grant a vote for a later term if the log isn't as complete" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(2, 2))(_ => false)
      "some raft node".replyTo(RequestVoteData(term = ourTerm + 1, lastLogIndex = 1, lastLogTerm = 2)) shouldBe RequestVoteReply(ourTerm, false)
    }
    "not grant a vote for a later term if the last log entry is for an earlier term" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(2, 2))(_ => false)
      "some raft node".replyTo(RequestVoteData(term = ourTerm + 1, lastLogIndex = 5, lastLogTerm = 1)) shouldBe RequestVoteReply(ourTerm, false)
    }
    "grant a vote if we haven't already cast a vote for this term, the term is greater than our term, and the log is at least as complete as ours" in {
      val ourTerm       = 2
      implicit val node = HasInfoForVote.const[String](term = ourTerm, log = LogCoords(2, 2))(_ => false)
      "some raft node".replyTo(RequestVoteData(term = ourTerm + 1, lastLogIndex = 2, lastLogTerm = 2)) shouldBe RequestVoteReply(ourTerm + 1, true)
    }
  }
}
