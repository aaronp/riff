package riff.raft.vertx.server
import monix.reactive.Observable
import riff.monix.RiffMonixSpec
import riff.raft.NodeId
import riff.raft.node.RoleCallback
import riff.raft.node.RoleCallback.NewLeaderEvent

class RunningVertxServiceTest extends RiffMonixSpec {
  "RunningVertxService.start" should {
    "connect 2 nodes" in {
      Given("Two running nodes")
      val runningNodes: Seq[RunningVertxService[String]] = List("node1", "node2").map { name =>
        val Some(node) = RunningVertxService.start(Array(name))
        node
      }

      Then("eventually one will become the leader")
      try {
        val leaderEvents = {
          val events: Seq[Observable[RoleCallback.RoleEvent]] = runningNodes.map(_.raft.stateCallback.events)
          val merged: Observable[RoleCallback.RoleEvent]      = Observable.fromIterable(events).merge
          merged.collect {
            case NewLeaderEvent(_, leaderId) => leaderId
          }
        }

        implicit val s          = runningNodes.head.scheduler
        val firstLeader: NodeId = leaderEvents.headL.runSyncUnsafe(testTimeout)
        firstLeader should not be null
      } finally {
        println("closing")
        runningNodes.foreach(_.close())
      }
    }
    "connect 2 nodes again" ignore {
      Given("Two running nodes")
      val runningNodes: Seq[RunningVertxService[String]] = List("node1", "node2").map { name =>
        val Some(node) = RunningVertxService.start(Array(name))
        node
      }

      Then("eventually one will become the leader")
      try {
        val leaderEvents = {
          val events: Seq[Observable[RoleCallback.RoleEvent]] = runningNodes.map(_.raft.stateCallback.events)
          val merged: Observable[RoleCallback.RoleEvent]      = Observable.fromIterable(events).merge
          merged.collect {
            case NewLeaderEvent(_, leaderId) => leaderId
          }
        }

        implicit val s          = runningNodes.head.scheduler
        val firstLeader: NodeId = leaderEvents.headL.runSyncUnsafe(testTimeout)
        firstLeader should not be null
      } finally {
        runningNodes.foreach(_.close())
      }
    }
  }

}