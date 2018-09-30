package riff.monix
import riff.RiffSpec

class MonixNodeTest extends RiffSpec {

  "MonixNode" should {
    "drive the cluster" in {

      // TODO - how do we drive the cluster via leader?
      val mn: Set[MonixNode[String]] = MonixNode.forNames[String]("a", "b", "c")
      mn.head

      Thread.sleep(10000)

    }
  }

}
