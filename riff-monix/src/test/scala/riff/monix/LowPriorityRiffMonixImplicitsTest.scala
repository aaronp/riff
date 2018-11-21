package riff.monix
import monix.reactive.Observable
import riff.reactive.AsPublisher

class LowPriorityRiffMonixImplicitsTest extends RiffMonixSpec {

  "takeWhileIncludeLast" should {
    "complete when the predicate completes but include the first result which returned false" in {

      withScheduler { implicit scheduler =>
        val integers: Observable[Int]    = Observable.fromIterable(0 to 10)
        val asp: AsPublisher[Observable] = AsPublisher[Observable]
        val pub                          = asp.takeWhileIncludeLast(integers)(_ != 4)

        val received = pub.toListL.runSyncUnsafe(testTimeout)
        received shouldBe List(0, 1, 2, 3, 4)
      }
    }
  }
}
