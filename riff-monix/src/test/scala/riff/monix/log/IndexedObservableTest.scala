package riff.monix.log
import monix.reactive.subjects.Var
import riff.monix.RiffMonixSpec

class IndexedObservableTest extends RiffMonixSpec {

  "IndexedObservable.fromIndex" should {
    "allow subscriptions to read from any arbitrary index" in {
      val map = collection.mutable.HashMap[Long, String]()
      def writeDown(i: Long, s: String) = {
        require(!map.contains(i))
        map += (i -> s)
      }
      def lookup(i: Long): String = map(i)

      val indexed = IndexedObservable[String](lookup)

      indexed.latest.foreach {
        case (idx, a) => writeDown(idx, a)
      }

      // populate our data
      val input = Var[String]("0")
      input.subscribe(indexed)

      (1 until 100).foreach { i =>
        input := i.toString
      }

      eventually {
        // the initial empty string plus our 99 elements
        map.size shouldBe 100
      }

      // now subscribe from some index
      val fromFifty = indexed.fromIndex(50)
      val list = fromFifty.take(10).toListL.runSyncUnsafe(testTimeout)
      list.map(_._1) shouldBe (50 until 60).toList

      input := "1000"
      input := "1001"
      input := "1002"

      val from98 = indexed.fromIndex(98).take(5).toListL.runSyncUnsafe(testTimeout)
      from98.size shouldBe 5
      from98 shouldBe List(98 -> "98", 99 -> "99", 100 -> "1000", 101 -> "1001", 102 -> "1002")
    }
  }
}
