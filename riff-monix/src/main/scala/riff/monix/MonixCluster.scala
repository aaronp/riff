package riff.monix
import riff.raft.node.{RaftCluster, RaftNode}

object MonixCluster {

  def of[A](nrNodes: Int)(newNode: String => RaftNode[A]): List[RaftNode[A]] = {
    val nodesNames = (1 to nrNodes).map { id =>
      s"node $id"
    }.toSet

    nodesNames.toList.map { name =>
      val cluster = RaftCluster(nodesNames - name)
      newNode(name).withCluster(cluster)
    }
  }

}
