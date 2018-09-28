package riff.raft.node
import riff.RiffSpec
import riff.raft.log.LogCoords
import riff.raft.messages.{RequestVote, RequestVoteResponse}

trait PersistentStateTCK extends RiffSpec {

  def withPersistentState(test: PersistentState => Unit) : Unit

  "PersistentState onRequestVote" should {
    "increment its term to the max of the request and its current term, even if it doesn't grant the vote" in {
      withPersistentState { ps =>
        val ourTerm = 2
        ps.currentTerm = ourTerm
        ps.currentTerm shouldBe ourTerm
        ps.hasAlreadyVotedInTerm(ourTerm + 1) shouldBe false

        val reply = ps.castVote(LogCoords(2, 2), "someone", RequestVote(term = ourTerm + 1, logState = LogCoords.Empty))
        reply shouldBe RequestVoteResponse(ourTerm + 1, false)

        // casting a 'false' vote doesn't count
        ps.hasAlreadyVotedInTerm(ourTerm + 1) shouldBe false
      }
    }
    "not grant a vote if it is for an earlier term" in {
      withPersistentState { ps =>
        val ourTerm = 2
        ps.currentTerm = ourTerm
        ps.currentTerm shouldBe ourTerm
        ps.hasAlreadyVotedInTerm(ourTerm - 1) shouldBe false

        val reply = ps.castVote(LogCoords(0, 0), "someone", RequestVote(term = ourTerm - 1, logState = LogCoords.Empty))
        reply shouldBe RequestVoteResponse(ourTerm, false)

        ps.hasAlreadyVotedInTerm(ourTerm - 1) shouldBe false
      }
    }

    "not grant a vote if we already cast a vote in the term" in {
      withPersistentState { ps =>
        val voteTerm = 2
        ps.currentTerm = voteTerm - 1

        ps.castVote(voteTerm, "dave")

        val request = RequestVote(term = voteTerm, logState = LogCoords.Empty)
        ps.castVote(LogCoords.Empty, "foo", request) shouldBe RequestVoteResponse(2, false)

        // verify we DO otherwise grant the vote if we haven't voted (just check we didn't return false for some other reason)
        ps.castVote(LogCoords.Empty, "foo", request.copy(term = voteTerm + 1)) shouldBe RequestVoteResponse(3, true)
        ps.votedFor(voteTerm) shouldBe Some("dave")
        ps.votedFor(voteTerm + 1) shouldBe Some("foo")
        ps.votedFor(voteTerm + 2) shouldBe None
        ps.currentTerm shouldBe 3

      }
    }
    "not grant a vote if it is for the same term" in {
      withPersistentState { ps =>
        val request = RequestVote(term = 1, logState = LogCoords.Empty)
        ps.castVote(LogCoords.Empty, "some candidate", request) shouldBe RequestVoteResponse(ps.currentTerm, true)
        ps.castVote(LogCoords.Empty, "some candidate", request) shouldBe RequestVoteResponse(ps.currentTerm, false)
      }
    }
    "not grant a vote for a later term if the log isn't as complete" in {
      withPersistentState { ps =>
        val request = RequestVote(term = 1, logState = LogCoords(2, 2))
        withClue("this candidate's request has a log entry which is one index behind ours") {

          val ourLogOnIndex3 = LogCoords(2, 3)
          val actual         = ps.castVote(ourLogOnIndex3, "some candidate", request)
          actual shouldBe RequestVoteResponse(ps.currentTerm, false)
        }
        val sameLog = LogCoords(2, 2)
        val actual  = ps.castVote(sameLog, "some candidate", request)
        actual shouldBe RequestVoteResponse(request.term, true)
      }
    }
    "not grant a vote for a term if the persistent state is in a later term" in {
      withPersistentState { ps =>
        ps.currentTerm = 5
        List(4 -> false, 5 -> true, 6 -> true).foreach {
          case (requestTerm, expected) =>
            val result = ps.castVote(LogCoords.Empty, "some candidate", RequestVote(term = requestTerm, logState = LogCoords(2, 2)))
            result shouldBe RequestVoteResponse(ps.currentTerm, expected)
        }
      }
    }
  }

}