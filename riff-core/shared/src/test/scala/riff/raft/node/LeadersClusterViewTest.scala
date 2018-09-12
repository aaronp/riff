package riff.raft.node
import riff.RiffSpec
import riff.raft.messages.AppendEntriesResponse

class LeadersClusterViewTest extends RiffSpec {

  "LeadersClusterView.update" should {
    "set the next index to matchIndex + 1 on success" in {
      val state: LeadersClusterView[String] = LeadersClusterView("node1" -> Peer.withUnmatchedNextIndex(3))
      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(3)

      state.update("node1", AppendEntriesResponse.ok(term = 1, matchIndex = 10)) shouldBe Some(Peer.withMatchIndex(10))

      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(11)
    }
    "decrement the next index on failure" in {
      val state: LeadersClusterView[String] = LeadersClusterView("node1" -> Peer.withUnmatchedNextIndex(3))
      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(3)

      state.update("node1", AppendEntriesResponse.fail(term = 1)) shouldBe Some(Peer.withUnmatchedNextIndex(2))

      state.stateForPeer("node1").map(_.nextIndex) shouldBe Some(2)
    }
    "ignore replies from nodes it doesn't know about" in {
      val state = LeadersClusterView("node1", "node2")
      state.update("unknown", AppendEntriesResponse.fail(term = 1)) shouldBe None
      state.toMap().keySet should contain only ("node1", "node2")
      state.numberOfPeers shouldBe 2
      state.stateForPeer("unknown") shouldBe empty
    }
  }
}
