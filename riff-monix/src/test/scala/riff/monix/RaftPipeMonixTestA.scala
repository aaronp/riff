package riff.monix

import monix.execution.Ack
import monix.reactive.Observable
import riff.raft._
import riff.raft.log.{LogAppendSuccess, LogCoords}
import riff.raft.messages._
import riff.raft.node._
import riff.raft.timer.LoggedInvocationClock

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Deadline

class RaftPipeMonixTestA extends RiffMonixSpec {

  "RaftPipeMonix" should {
    import monix.execution.Scheduler.Implicits.global
    "construct an endpoint which we can used to communicate w/ other endpoints" in {

//      withScheduler { implicit scheduler =>
      Given("Two raft nodes linked together")

      implicit val everyonesClock = new LoggedInvocationClock // manually trigger clock events

      val cluster = RaftPipeMonix.asCluster[String](RaftNode.inMemory("a"), RaftNode.inMemory("b"))

      try {
        When("One becomes the leader")
        val List(leader, follower) = cluster.values.toList

        leader.input.onNext(ReceiveHeartbeatTimeout).futureValue

        Then("Eventually one will become leader")
        eventually {
          leader.handler.state().isLeader
          leader.handler.currentTerm() shouldBe follower.handler.currentTerm()
        }

        val appendResultPublisher: Observable[AppendStatus] = {
          val client: RaftClient[Observable, String] = leader.client

          client.append("Hello", "World").dump("Client Resp")
        }

//          val lastUpdate = appendResultPublisher.lastL.runSyncUnsafe(testTimeout)
        val received  = ListBuffer[AppendStatus]()
        var completed = false
        appendResultPublisher.doOnComplete(() => completed = true).foreach { status =>
          received += status
        }
        eventually {
          withClue(s"Append Never completed after having received ${received.size} updates: [${received.mkString(",")}]") {
            completed shouldBe true
          }
        }
        val lastUpdate = received.last

        lastUpdate shouldBe AppendStatus(
          leaderAppendResult = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 2), List()),
          appended = Map("a" -> AppendEntriesResponse(1, true, 2), "b" -> AppendEntriesResponse(1, true, 2)),
          appendedCoords = Set(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 2)),
          clusterSize = 2,
          false
        )

        eventually {
          val secondaryValues: Array[String] = follower.handler.log.entriesFrom(1).map(_.data)
          secondaryValues should contain inOrderOnly ("Hello", "World")
        }

        eventually {
          leader.handler.log.latestCommit() shouldBe 2
        }

        // and, just for fun, ensure we replicate to the follower by poking our manual clock
        leader.input.onNext(SendHeartbeatTimeout)
        eventually {
          follower.handler.log.latestCommit() shouldBe 2
        }

      } finally {
        cluster.values.foreach(_.close())
//        }
      }
    }
  }
}
