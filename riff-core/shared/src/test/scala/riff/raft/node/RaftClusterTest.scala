package riff.raft.node
import riff.RiffSpec

class RaftClusterTest extends RiffSpec {

  "RaftCluster" should {
    "improve test coverage" in {
      RaftCluster(1.toString,2.toString,3.toString).toString shouldBe "4 node cluster (this node plus 3 peers: [1,2,3])"
    }
  }
}
