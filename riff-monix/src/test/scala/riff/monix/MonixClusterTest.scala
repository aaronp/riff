package riff.monix
import monix.reactive.Observable
import riff.raft.NodeId
import riff.raft.node.RoleCallback.NewLeaderEvent

class MonixClusterTest extends RiffMonixSpec {

//  "MonixCluster" should {
//    "elect a leader" in {
//      val cluster = MonixCluster.forNames[String]("a", "b", "c").start()
//
////      cluster.nodes.foreach { n =>
////        n.observableLog.get.committedEntries.dump(s"\t${n.nodeId} committed").foreach(_ => ())
////        n.observableLog.get.appendedEntries().dump(s"\t${n.nodeId} appended").foreach(_ => ())
////        n.observableState.dump(s"---------> ${n.nodeId} st8").foreach(_ => ())
////      }
//
//      // observe each of the nodes' observableState and quit as soon as we see the first NewLeaderEvent
//      //
//      val leader: MonixNode[String] = {
//        val leaderAlerts: Set[Observable[NodeId]] = cluster.nodes.map { n =>
//          n.observableState.collect {
//            case NewLeaderEvent(_, name) => name
//          } //.dump(s"${n.nodeId} notices leader")
//        }
//        val firstToSeeLeader: Observable[NodeId] = leaderAlerts.reduce(_ ambWith _)
//
//        val leaderId = firstToSeeLeader.take(1).headL.runSyncUnsafe(testTimeout * 10)
//        cluster.nodeFor(leaderId).get
//
//      }
//      cluster.leader().map(_.nodeId) shouldBe Some(leader.nodeId)
//
//      val res = leader.client.append("hello", "world!") //.dump("append Result")
//
//      // TODO = assert the append status
////      res.dump("!!!!").foreach { status =>
////        //
////        println(status)
////      }
//
//      eventually {
//        cluster.nodes.foreach { node =>
//          node.log().latestCommit() shouldBe 2
//          node.log().entriesFrom(1, 2).map(_.data) should contain inOrderOnly ("hello", "world!")
//        }
//      }
//    }
//  }

}
