package riff.monix

class MonixClusterTest extends RiffMonixSpec {

  "MonixCluster" should {
    "elect a leader" in {
      val cluster = MonixCluster.forNames[String]("a", "b", "c").start()

//      cluster.nodes.foreach { n =>
//        n.observableLog.get.committedEntries.dump(s"${n.nodeId} committed").foreach(_ => ())
//        n.observableLog.get.appendedEntries().dump(s"${n.nodeId} appended").foreach(_ => ())
//      }

      val deadline = (testTimeout * 4).fromNow
      var leader: MonixNode[String] = null
      while (!deadline.isOverdue() && leader == null) {
        cluster.leader() match {
          case Some(value) => leader = value
          case None => Thread.`yield`()
        }
      }

      leader should not be (null)
      val res = leader.client.append("hello", "world!").dump("append Result")

      // TODO = assert the append status
      res.dump("!!!!").foreach { status =>
        println(status)
      }

      eventually {
        cluster.nodes.foreach { node =>
          node.log().latestCommit() shouldBe 2
          node.log().entriesFrom(1, 2).map(_.data) should contain inOrderOnly ("hello", "world!")
        }
      }
    }
  }

}
