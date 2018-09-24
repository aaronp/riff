package riff.raft.timer
import org.scalatest.matchers.Matcher
import riff.RiffSpec

import scala.concurrent.duration._

class RandomTimerTest extends RiffSpec {

  "RandomTimer" should {
    "produce numbers in a range" in {
      val rand = new RandomTimer(100.millis, 0.25)

      val values = (0 to 1000).map { _ => rand.next().toMillis.toInt
      }

      val beBetween75And125: Matcher[Int] = be >= 75 and be <= 125
      values.foreach { _ should beBetween75And125 }
    }
  }
}
