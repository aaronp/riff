package riff.raft.node
import riff.RiffSpec

class RaftClusterTest extends RiffSpec {

  "RaftCluster" should {
    "improve test coverage" in {
      RaftCluster(1.toString,2.toString,3.toString).toString shouldBe "4 node cluster (this node plus 3 peers: [1,2,3])"
    }
  }
  "RaftCluster.dynamic" should {
    "improve test coverage" in {
      val cluster = RaftCluster.dynamic("original")
      cluster.peers should contain only("original")

      cluster.add("new node")
      cluster.peers should contain only("original", "new node")

      cluster.remove("original")
      cluster.peers should contain only("new node")
      cluster.contains("original") shouldBe false
      cluster.contains("meh") shouldBe false
      cluster.contains("new node") shouldBe true
    }
  }
}
