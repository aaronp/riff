package riff.monix
import monix.reactive.Observable
import org.scalatest.FunSuite
import riff.raft.log.LogAppendResult

class RiffMonixClientTest extends RiffMonixSpec {

  "RiffMonixClient" should {
    "complete w/ an error if the log observable ends up replacing the very coords we've appended" in {
      ???
    }
  }
}
