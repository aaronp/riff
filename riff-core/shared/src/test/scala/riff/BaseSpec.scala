package riff

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{GivenWhenThen, Matchers, WordSpec}
import riff.raft.log.{LogCoords, RaftLog}
import riff.raft.messages._
import riff.raft.node._
import riff.raft.timer.LoggedInvocationClock

import scala.collection.immutable
import scala.concurrent.duration._

class BaseSpec extends WordSpec with Matchers with ScalaFutures with GivenWhenThen {

  /**
    * All the timeouts!
    */
  implicit def testTimeout: FiniteDuration = 15.seconds

  def testClassName = getClass.getSimpleName.filter(_.isLetterOrDigit)

  implicit override def patienceConfig =
    PatienceConfig(timeout = scaled(Span(testTimeout.toSeconds, Seconds)), interval = scaled(Span(150, Millis)))

  def logWithCoords(coords: LogCoords): RaftLog[Int] = BaseSpec.logWithCoords(coords)

  case class TestCluster(ofSize: Int) {

    lazy val byName: Map[String, RaftNode[Int]] =
      clusterNodes.map(n => n.nodeId -> n).toMap.ensuring(_.size == clusterNodes.size)

    val clusterNodes: List[RaftNode[Int]] = {
      val nodes = (1 to ofSize).map { i => newNode(s"node $i")
      }
      val nodeNames = nodes.map(_.nodeId).toSet
      nodes.map { n => n.withCluster(RaftCluster(nodeNames - n.nodeId))
      }.toList
    }

    def testTimerFor(nodeKey: String): LoggedInvocationClock =
      byName(nodeKey).timers.clock.asInstanceOf[LoggedInvocationClock]

    /** Convenience method to:
      * 1) timeout the given node
      * 2) apply the vote requests to the other members
      * 3) apply the vote responses to the given node
      *
      * @param member the member to transition
      * @return the heartbeat messages intended to be applied to the remaining members
      */
    def electLeader(member: String): Map[String, AppendEntries[Int]] = electLeader(byName(member))

    def electLeader(member: RaftNode[Int]): Map[String, AppendEntries[Int]] = {
      val leaderResultsAfterHavingAppliedTheResponses = attemptToElectLeader(member).map(_._2)
      asHeartbeats(leaderResultsAfterHavingAppliedTheResponses)
    }

    /** Convenience method to:
      * 1) timeout the given node
      * 2) apply the vote requests to the other members
      *
      * @param member the member to transition
      * @return the vote responses
      */
    def attemptToElectLeader(candidate: RaftNode[Int]): List[(RaftResponse, candidate.Result)] = {
      val AddressedRequest(requestVotes) = candidate.onTimerMessage(ReceiveHeartbeatTimeout)
      val replies: Map[String, RaftNode[Int]#Result] = sendMessages(candidate.nodeId, requestVotes)

      replies.toList.map {
        case (from, AddressedResponse(_, voteResponse)) =>
          val candidateResp = candidate.onMessage(AddressedMessage(from, voteResponse))
          voteResponse -> candidateResp
        case other => fail(s"Expected an AddressedResponse but got $other")
      }
    }

    // convenience method for converting leader replies to hb messages
    def asHeartbeats(leaderResultsAfterHavingAppliedTheResponses: immutable.Iterable[RaftNodeResult[Int]]) = {
      val all = leaderResultsAfterHavingAppliedTheResponses.collect {
        case AddressedRequest(requests) => requests
      }
      val flat: immutable.Iterable[(String, RaftRequest[Int])] = all.flatten
      flat.toMap.ensuring(_.size == flat.size).mapValues {
        case append @ AppendEntries(LogCoords.Empty, 1, 0, entries) =>
          entries shouldBe empty
          append
        case other => fail(s"Expected an append entries but got $other")
      }
    }

    def sendMessages(
      originator: String,
      requests: Iterable[(String, RaftRequest[Int])]): Map[String, RaftNode[Int]#Result] = {
      val all = requests.toList.collect {
        case (name, req) if name != originator => name -> byName(name).onMessage(AddressedMessage(originator, req))
      }
      all.toMap.ensuring(
        _.size == all.size,
        "test case doesn't support multiple requests from the same node - do it in separate calls")
    }

    def sendResponses(responses: Map[String, RaftNode[Int]#Result]) = {
      val all = responses.collect {
        case (from, AddressedResponse(backTo, resp)) => from -> byName(backTo).onMessage(AddressedMessage(from, resp))
      }
      all.ensuring(
        _.size == all.size,
        "test case doesn't support multiple responses from the same node - do it in separate calls")
    }

  }
  protected def newNode(name: String = "test"): RaftNode[Int] = {
    implicit val timer = new LoggedInvocationClock
    RaftNode.inMemory[Int](name)
  }
}

object BaseSpec {

  def logWithCoords(coords: LogCoords): RaftLog[Int] = {
    (1 to coords.index).foldLeft(RaftLog.inMemory[Int]()) {
      case (log, i) =>
        log.append(coords.copy(index = i), 999 + i)
        log
    }
  }
}
