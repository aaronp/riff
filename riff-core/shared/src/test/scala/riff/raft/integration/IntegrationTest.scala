package riff.raft.integration
import riff.RiffSpec
import riff.raft.node.Leader

class IntegrationTest extends RiffSpec {
  "Raft Cluster" should {
    "elect a leader event with a cluster of 1" in {
      val simulator = RaftSimulator.clusterOfSize(1)
      simulator.leader() shouldBe empty
      val Some(result) = simulator.advance()
      result.afterState(1).role shouldBe Leader
      simulator.leader() should not be (empty)
    }
    "dynamically add a node" in {
      val simulator = RaftSimulator.clusterOfSize(1)

      simulator
    }
  }
}
