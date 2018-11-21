package riff.raft.node
import riff.RiffSpec

class PeerTest extends RiffSpec {

  "Peer.setMatchIndex" should {
    "set its next index to be matchIndex + 1" in {
      Peer.Empty.setMatchIndex(9).matchIndex shouldBe 9
      Peer.Empty.setMatchIndex(9).nextIndex shouldBe 10
    }
  }
  "Peer.withUnmatchedNextIndex" should {
    "not allow negative indices" in {
      intercept[Exception] {
        Peer.withUnmatchedNextIndex(-1)
      }
    }
  }
  "Peer.withMatchIndex" should {
    "not allow negative indices" in {
      intercept[Exception] {
        Peer.withMatchIndex(-1)
      }
    }
  }
  "Peer.toString" should {
    "be intuitive" in {
      Peer.Empty.setMatchIndex(9).toString shouldBe s"Peer(nextIndex=10, matchIndex=9)"
    }
  }
  "Peer.equals" should {
    "equate two peers" in {

      Peer.withUnmatchedNextIndex(3) shouldEqual Peer.withUnmatchedNextIndex(3)
      Peer.withUnmatchedNextIndex(3).hashCode() shouldEqual Peer.withUnmatchedNextIndex(3).hashCode()

      Peer.withUnmatchedNextIndex(3) should not equal (Peer.withUnmatchedNextIndex(4))
      Peer.withUnmatchedNextIndex(3).hashCode() should not equal (Peer.withUnmatchedNextIndex(4).hashCode())

      Peer.withMatchIndex(3) shouldEqual Peer.withMatchIndex(3)
      Peer.withMatchIndex(3).hashCode shouldEqual Peer.withMatchIndex(3).hashCode

      Peer.withMatchIndex(3) should not equal (Peer.withUnmatchedNextIndex(3))
      Peer.withMatchIndex(3).hashCode() should not equal (Peer.withUnmatchedNextIndex(3).hashCode())

      Peer.withMatchIndex(3) should not equal (Peer.withMatchIndex(4))
      Peer.withMatchIndex(3).hashCode() should not equal (Peer.withMatchIndex(4).hashCode())

      Peer.withMatchIndex(3) should not equal ("banana")
    }
  }
}
