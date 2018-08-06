package riff

import java.nio.file.Path
import java.util.UUID

import eie.io.LowPriorityIOImplicits
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}
import riff.raft.log.{LogCoords, RaftLog}
import riff.raft.messages.{AppendEntries, RaftRequest, ReceiveHeartbeatTimeout}
import riff.raft.node._
import riff.raft.timer.LoggedInvocationTimer

import scala.collection.immutable
import scala.concurrent.duration._

abstract class RiffSpec extends WordSpec with Matchers with ScalaFutures with GivenWhenThen with LowPriorityIOImplicits {

  /**
    * All the timeouts!
    */
  implicit def testTimeout: FiniteDuration = 2.seconds

  /**
    * @return the timeout for something NOT to happen
    */
  def testNegativeTimeout: FiniteDuration = 300.millis

  def testClassName = getClass.getSimpleName.filter(_.isLetterOrDigit)

  implicit override def patienceConfig =
    PatienceConfig(timeout = scaled(Span(testTimeout.toSeconds, Seconds)), interval = scaled(Span(150, Millis)))

  def nextTestDir(name: String) = {
    s"target/test/${name}-${UUID.randomUUID()}".asPath
  }

  def withDir[T](f: Path => T): T = {
    val name: String = getClass.getSimpleName
    val dir: Path    = nextTestDir(name)
    if (dir.exists()) {
      dir.delete()
    }
    dir.mkDirs()
    try {
      f(dir)
    } finally {
      dir.delete()
    }
  }

  def logWithCoords(coords: LogCoords): RaftLog[Int] = RiffSpec.logWithCoords(coords)

  case class TestCluster(ofSize: Int) {

    lazy val byName = clusterNodes.map(n => n.nodeKey -> n).toMap.ensuring(_.size == clusterNodes.size)

    val clusterNodes: List[NodeState[String, Int]] = {
      val nodes = (1 to ofSize).map { i =>
        newNode(s"node $i")
      }

      import riff.raft.node.RichNodeState._
      val nodeNames = nodes.map(_.nodeKey).toSet
      nodes.map { n =>
        n.withCluster(RaftCluster(nodeNames - n.nodeKey))
      }.toList
    }

    def testTimerFor(nodeKey: String) = byName(nodeKey).timers.timer.asInstanceOf[LoggedInvocationTimer[String]]

    /** Convenience method to:
      * 1) timeout the given node
      * 2) apply the vote requests to the other members
      * 3) apply the vote responses to the given node
      * @param member the member to transition
      * @return the heartbeat messages intended to be applied to the remaining members
      */
    def electLeader(member: String): Map[String, AppendEntries[Int]] = {
      val candidate                      = byName(member)
      val AddressedRequest(requestVotes) = candidate.onTimerMessage(ReceiveHeartbeatTimeout)
      val replies                        = sendMessages(member, requestVotes)

      val leaderResultsAfterHavingAppliedTheResponses = replies.map {
        case (from, AddressedResponse(_, voteResponse)) => candidate.onMessage(from, voteResponse)
      }
      asHeartbeats(leaderResultsAfterHavingAppliedTheResponses)
    }

    // convenience method for converting leader replies to hb messages
    def asHeartbeats(leaderResultsAfterHavingAppliedTheResponses: immutable.Iterable[NodeStateOutput[String, Int]]) = {
      val all = leaderResultsAfterHavingAppliedTheResponses.collect {
        case AddressedRequest(requests) => requests
      }
      val flat: immutable.Iterable[(String, RaftRequest[Int])] = all.flatten
      flat.toMap.ensuring(_.size == flat.size).mapValues {
        case append @ AppendEntries(LogCoords.Empty, 1, 0, entries) =>
          entries shouldBe empty
          append
      }
    }

    def sendMessages(originator: String, requests: Iterable[(String, RaftRequest[Int])]) = {
      val all = requests.toList.collect {
        case (name, req) if name != originator => name -> byName(name).onMessage(originator, req)
      }
      all.toMap.ensuring(_.size == all.size, "test case doesn't support multiple requests from the same node - do it in separate calls")
    }

  }
  protected def newNode(name: String = "test"): NodeState[String, Int] = {
    implicit val timer = new LoggedInvocationTimer[String]
    NodeState.inMemory[String, Int](name)
  }
}

object RiffSpec {
  def logWithCoords(coords: LogCoords): RaftLog[Int] = {
    (1 to coords.index).foldLeft(RaftLog.inMemory[Int]()) {
      case (log, i) =>
        log.append(coords.copy(index = i), 999 + i)
        log
    }
  }
}
