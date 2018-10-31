package riff
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.reactive.newContextWithThreadPrefix

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration._

class RiffThreadedSpec extends RiffSpec with BeforeAndAfterAll with Eventually {

  implicit override def testTimeout: FiniteDuration = 8.seconds

  implicit protected val execCtxt: ExecutionContextExecutorService = newContextWithThreadPrefix(getClass.getSimpleName)

  override def afterAll() = {
    execCtxt.shutdown()
  }

}
