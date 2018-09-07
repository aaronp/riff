package riff.raft.node
import riff.RiffSpec
import riff.raft.messages.AppendEntriesResponse

class LeaderStateTest extends RiffSpec {

  "LeaderState.update" should {
    "set the next index to matchIndex + 1 on success" in {
      val state: LeaderState[String] = LeaderState("node1" -> Peer(nextIndex = 3, matchIndex = 0))
      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(3)

      state.update("node1", AppendEntriesResponse.ok(term = 1, matchIndex = 10)) shouldBe Some(Peer(nextIndex = 11, matchIndex = 10))

      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(11)
    }
    "decrement the next index on failure" in {
      val state: LeaderState[String] = LeaderState("node1" -> Peer(nextIndex = 3, matchIndex = 0))
      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(3)

      state.update("node1", AppendEntriesResponse.fail(term = 1)) shouldBe Some(Peer(nextIndex = 2, matchIndex = 0))

      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(2)
    }
    "ignore replies from nodes it doesn't know about" in {
      val state = LeaderState("node1", "node2")
      state.update("unknown", AppendEntriesResponse.fail(term = 1)) shouldBe empty
      state.stateForPeer("unknown") shouldBe empty
    }
  }
}
