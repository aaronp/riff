package riff.monix
import riff.raft.AppendStatus
import riff.raft.messages.ReceiveHeartbeatTimeout
import riff.raft.timer.LoggedInvocationClock

import scala.collection.mutable.ListBuffer

class MonixClientTest extends RiffMonixSpec {

  "MonixClient.append" should {

    "send notifications when the append responses are received" in {

      withScheduler { implicit scheduler =>
        implicit val clock = new LoggedInvocationClock

        val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)

        try {

          val head = fiveNodeCluster.values.head
          head.input.onNext(ReceiveHeartbeatTimeout)

          val leader = eventually {
            fiveNodeCluster.values.find(_.handler.state().isLeader).get
          }

          var done     = false
          val received = ListBuffer[AppendStatus]()
          leader.client
            .append("XYZ123")
            .doOnComplete { () =>
              done = true
            }
            .foreach { x =>
              received += x
            }

          eventually {
            withClue(s"The append stream never completed after having received: ${received.mkString("; ")}") {
              done shouldBe true
            }
          }
        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
      }
    }
  }
}
