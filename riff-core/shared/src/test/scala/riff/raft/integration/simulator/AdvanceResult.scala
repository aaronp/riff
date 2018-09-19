package riff.raft.integration
package simulator

import riff.raft.integration.simulator.RaftSimulator.NodeResult
import riff.raft.node.{Follower, Leader, NodeRole}

/**
  * The result of having advanced the [[Timeline]] via the [[RaftSimulator]]
  */
case class AdvanceResult(node: String,
                         beforeStateByName: Map[String, NodeSnapshot[String]],
                         beforeTimeline: Timeline[TimelineType],
                         advanceEvents: List[(TimelineType, NodeResult)],
                         afterTimeline: Timeline[TimelineType],
                         afterStateByName: Map[String, NodeSnapshot[String]])
    extends HasTimeline[TimelineType] {

  def nodesWithRole(role: NodeRole): Iterable[NodeSnapshot[String]] = afterStateByName.values.filter(_.role == role)
  def leader: NodeSnapshot[String] = {
    val List(only) = nodesWithRole(Leader).toList
    only
  }

  /** @return true when there is an elected leader and the remaining nodes are all followers in the same term
    */
  def hasLeader: Boolean = {
    nodesWithRole(Leader).toList match {
      case Nil => false
      case List(leader) =>
        val followers = nodesWithRole(Follower)
        followers.size == leader.cluster.peers.size && followers.forall(_.persistentStateSnapshot.currentTerm == leader.persistentStateSnapshot.currentTerm)
    }
  }


  /** @param idx the member index
    * @return the 'before' node state for the given member
    */
  def beforeState(idx: Int): NodeSnapshot[String] = beforeStateByName(nameForIdx(idx))

  /** @param idx the member index
    * @return the 'after' node state for the given member
    */
  def afterState(idx: Int): NodeSnapshot[String] = afterStateByName(nameForIdx(idx))

  def stateChanges: Map[String, NodeSnapshot[String]] = afterStateByName.filterNot {
    case (key, st8) => beforeStateByName(key) == st8
  }

  override def toString = {

    val result = events.zipWithIndex
      .map {
        case ((input, res), i) =>
          s"""INPUT ${i + 1}:
         |$input
         |
         |OUTPUT ${i + 1}:
         |$res
       """.stripMargin
      }
      .mkString("\n")

    val newEvents = afterTimeline.diff(beforeTimeline)
    s"""$node processed ${advanceEvents.size}:
       |
       |$result
       |
       |Before State:
       |${beforeStateByName.mapValues(_.pretty()).mkString("\n")}
       |${stateChanges.size} Changes:
       |${stateChanges.mapValues(_.pretty()).mkString("\n")}
       |
       |Before Timeline:
       |${beforeTimeline.pretty("    ")}
       |After Timeline:
       |${afterTimeline.pretty("    ")}
       |
       |with ${newEvents.size} new enqueued events: ${newEvents.mkString("\n")}""".stripMargin
  }
  override def currentTimeline() = afterTimeline
}

object AdvanceResult {
  def apply(node: String,
            beforeStateByName: Map[String, NodeSnapshot[String]],
            beforeTimeline: Timeline[TimelineType],
            event: TimelineType,
            result: NodeResult,
            afterTimeline: Timeline[TimelineType],
            undeliveredTimeline: Timeline[TimelineType],
            afterStateByName: Map[String, NodeSnapshot[String]]) = {
    new AdvanceResult(node, beforeStateByName, beforeTimeline, List(event -> result), afterTimeline, afterStateByName)
  }
}
