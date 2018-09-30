package riff.monix
import org.scalatest.FunSuite
import riff.RiffSpec

import RiffSchedulers.computation.scheduler

class RouterTest extends RiffSpec {

  "Router" should {
    "send messages between components" in {

      val names = Set("A", "B", "C")
      val router: Router[(String, Int), (String, Int)] = Router[(String, Int)](names)

      names.foreach { name =>
        val peers = names - name
        peers.foreach { peer =>
          val pipe = router.pipes(name)
          pipe.output
        }
      }
    }
  }
}
