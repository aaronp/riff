package riff.monix
import monix.execution.Scheduler
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import riff.RiffSpec

abstract class RiffMonixSpec extends RiffSpec with Eventually with BeforeAndAfterAll {

  implicit val scheduler = Scheduler.computation()

  override def afterAll(): Unit = {
    scheduler.shutdown()
  }

}
