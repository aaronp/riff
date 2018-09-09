package riff.raft.integration
import riff.RiffSpec

class IntegrationTest extends RiffSpec {
  "Raft Cluster" should {
    "elect a leader event with a cluster of 1" in {
      val simulator = RaftSimulator.clusterOfSize(1)
      simulator.leader() shouldBe empty
      val Some(result) = simulator.advance()
      println(result)
      simulator.leader() should not be (empty)
    }
  }
}
