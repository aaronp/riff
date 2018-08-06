package riff.monix.log
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.monix.RiffSchedulers.DefaultScheduler
import riff.raft.log.{LogAppendResult, LogCoords, RaftLog}

import scala.collection.mutable.ListBuffer

class ObservableLogTest extends RiffSpec with Eventually {

  "ObservableLog.asObservable" should {
    "notify observers when an entry is committed" in {
      Given("An observable log with an observer")
      val underlying = RaftLog.inMemory[String]()
      val ol         = ObservableLog(underlying)

      val received = ListBuffer[LogNotificationMessage]()
      ol.asObservable.dump("debug").foreach { msg =>
        received += msg
      }

      When("three initial entries are appended")
      ol.append(LogCoords(2, 1), "first", "second", "third")

      Then("The observer should be notified of a LogAppendResult")
      eventually {
        received should contain only (LogAppended(LogAppendResult(term = 2, firstIndex = 1, lastIndex = 3, Nil)))
      }

      // -- clean up for convenience in testing/asserting new entriew
      received.clear()

      When("the first entry is committed")
      ol.commit(1)

      Then("The observer should receive a commit message")
      eventually {
        received should contain only (LogCommitted(Seq(LogCoords(2, 1))))
      }
      received.clear()

      When("Another entry is appended in another term")
      ol.append(LogCoords(10, 4), "first entry in term 10")

      Then("The observer should be notified of a LogAppendResult")
      eventually {
        received should contain only (LogAppended(LogAppendResult(term = 10, firstIndex = 4, lastIndex = 4, Nil)))
      }
      received.clear()

      When("the remaining entries are committed")
      ol.commit(4)

      Then("The observer should receive a commit message")
      eventually {
        received should contain only (LogCommitted(Seq(LogCoords(2, 2), LogCoords(2, 3), LogCoords(10, 4))))
      }
    }
    "notify observers when an entry is appended" in {
      val underlying = RaftLog.inMemory[String]()
      val ol         = ObservableLog(underlying)

      val listTask = ol.asObservable.take(1).toListL

      ol.append(LogCoords(2, 1), "first commit")

      val List(LogAppended(appendResult)) = listTask.runSyncUnsafe(testTimeout)
      appendResult.firstIndex shouldBe 1
      appendResult.lastIndex shouldBe 1
      appendResult.term shouldBe 2
      appendResult.replacedIndices shouldBe empty
    }
    "notify observers when multiple entries are appended" in {
      val underlying = RaftLog.inMemory[String]()
      val ol         = ObservableLog(underlying)

      val received = ListBuffer[LogNotificationMessage]()
      ol.asObservable.dump("debug").foreach { msg =>
        received += msg
      }

      val result1: LogAppendResult = ol.append(LogCoords(2, 1), "first", "second", "third")
      result1 shouldBe LogAppendResult(term = 2, firstIndex = 1, lastIndex = 3, Nil)
      ol.latestAppended() shouldBe LogCoords(2, 3)

      val result2: LogAppendResult = ol.append(LogCoords(3, 2), "replaced second", "replaced third", "new fourth")
      result2 shouldBe LogAppendResult(term = 3, firstIndex = 2, lastIndex = 4, replacedIndices = Seq(2, 3))

      eventually {
        received.size shouldBe 2
      }
      val List(LogAppended(appendResult1), LogAppended(appendResult2)) = received.toList
      appendResult1 shouldBe result1
      appendResult2 shouldBe result2
    }
  }
}
