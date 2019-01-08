package riff.raft.rest
import monix.reactive.Observable
import monix.reactive.subjects.Var
import riff.monix.RiffMonixSpec
import riff.monix.log.ObservableLog._
import riff.monix.log.{LogStatus, ObservableLog}
import riff.raft.log.{LogAppendSuccess, LogCoords, RaftLog}

class GetLogStatusTest extends RiffMonixSpec {

  "Var" should {
    "send the latest value" in {
      withScheduler { implicit s =>
        val vr = Var(1)

        val sub = vr.dump("vr").filter(_ > 0).flatMap(i => Observable.fromIterable(List(i)))
        def latest() = {
          sub.headL.runSyncUnsafe(testTimeout)
        }

        val expected = 2 to 10
        val actual = expected.map { i =>
          vr := i
          eventually {
            latest() shouldBe i
          }
          latest()
        }
        actual.toList shouldBe expected.toList
      }
    }
  }
  "GetLogStatus" should {
    "return an empty initial result" in {
      withScheduler { implicit s =>
        val log: ObservableLog[String] = RaftLog.inMemory[String]().observable
        val instanceUnderTest          = GetLogStatus.future(log.status())
        instanceUnderTest.getLogStatus().futureValue shouldBe LogStatus(LogCoords.Empty, LogCoords.Empty)
      }
    }
    "return the last appended result" in {
      withScheduler { implicit s =>
        Given("An observable log")
        val log: ObservableLog[String] = RaftLog.inMemory[String]().observable
        val instanceUnderTest          = GetLogStatus.future(log.status())
        instanceUnderTest.getLogStatus().futureValue shouldBe LogStatus(LogCoords.Empty, LogCoords.Empty)

        When("And entry is appended")
        log.append(LogCoords(2, 1), "first") shouldBe LogAppendSuccess(LogCoords(2, 1), LogCoords(2, 1))

        Then("getLogStatus should return an updated log status w/ an updated lastAppended value")
        eventually {
          instanceUnderTest.getLogStatus().futureValue shouldBe LogStatus(lastAppended = LogCoords(2, 1), LogCoords.Empty)
        }
      }
    }
    "return the last committed result" in {
      withScheduler { implicit s =>
        Given("An observable log")
        val log: ObservableLog[String] = RaftLog.inMemory[String]().observable
        val instanceUnderTest          = GetLogStatus.future(log.status())
        instanceUnderTest.getLogStatus().futureValue shouldBe LogStatus(LogCoords.Empty, LogCoords.Empty)

        When("And entry is appended")
        log.append(LogCoords(100, 1), "first", "second", "third") shouldBe LogAppendSuccess(LogCoords(100, 1), LogCoords(100, 3))

        And("And entry is committed")
        log.commit(2) should not be empty

        Then("getLogStatus should return an updated log status")
        eventually {
          instanceUnderTest.getLogStatus().futureValue shouldBe LogStatus(lastAppended = LogCoords(100, 3), lastCommitted = LogCoords(100, 2))
        }
      }
    }
  }
}
