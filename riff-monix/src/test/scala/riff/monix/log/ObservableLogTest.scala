package riff.monix.log
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.monix.RiffSchedulers.DefaultScheduler
import riff.raft.log.{LogAppendResult, LogCoords, RaftLog}

import scala.collection.mutable.ListBuffer

class ObservableLogTest extends RiffSpec with Eventually {
  import ObservableLog._

  "ObservableLog.committedEntriesFrom" should {
    "not feed entries until they are committed" in {
      val log       = RaftLog.inMemory[String]().asObservable
      val committed = log.committedEntriesFrom(0)

      val entries = ListBuffer[(LogCoords, String)]()
      committed.foreach { e => //
        entries += e
      }

      log.append(LogCoords(10, 1), "first")
      log.append(LogCoords(10, 2), "second")
      log.append(LogCoords(10, 3), "third")
      entries shouldBe empty

      log.commit(2)

      eventually {
        entries should contain only (LogCoords(10, 1) -> "first", LogCoords(10, 2) -> "second")
      }
    }
  }
  "ObservableLog.committedCoordsFrom" should {
    "read from events starting in the future when given an index > the current max appended" in {
      val log = RaftLog.inMemory[String]().asObservable

      val received = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(5).foreach { entry => //
        received += entry
      }

      log.append(LogCoords(2, 1), "first", "second", "third")
      received shouldBe empty
      log.append(LogCoords(3, 4), "fourth", "fifth", "sixth")
      eventually {
        received should contain only (
          LogCoords(3, 5) -> "fifth", //
          LogCoords(3, 6) -> "sixth" //
        )
      }
    }
    "read from past events" in {
      Given("A observable log")
      val log = RaftLog.inMemory[String]().asObservable

      When("we subscribe to entries from index 2 BEFORE any have been appended")
      val beforeList = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(2).take(3).foreach { before => //
        beforeList += before
      }

      And("append some entries to the log")
      log.append(LogCoords(2, 1), "first", "second", "third")
      log.append(LogCoords(3, 4), "fourth", "fifth")

      Then("we should see the entries starting at index 2")
      eventually {
        beforeList should contain only (//
        LogCoords(2, 2) -> "second", //
        LogCoords(2, 3) -> "third", //
        LogCoords(3, 4) -> "fourth" //
        )
      }

      When("We now make another subscription from index 2, as the entries already exist in the log")
      // the entries already exist - now
      val afterList = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(2).take(3).foreach { after => //
        afterList += after
      }

      Then("We should get the same results as when we observed prior to appending entries")
      eventually {
        afterList.toList shouldBe beforeList.toList
      }
    }
  }

  "ObservableLog.appendedEntriesFrom" should {
    "read from events starting in the future when given an index > the current max appended" in {
      val log = RaftLog.inMemory[String]().asObservable

      val received = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(5).foreach { entry => //
        received += entry
      }

      log.append(LogCoords(2, 1), "first", "second", "third")
      received shouldBe empty
      log.append(LogCoords(3, 4), "fourth", "fifth", "sixth")
      eventually {
        received should contain only (
          LogCoords(3, 5) -> "fifth", //
          LogCoords(3, 6) -> "sixth" //
        )
      }
    }
    "read from past events" in {
      Given("A observable log")
      val log = RaftLog.inMemory[String]().asObservable

      When("we subscribe to entries from index 2 BEFORE any have been appended")
      val beforeList = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(2).take(3).foreach { before => //
        beforeList += before
      }

      And("append some entries to the log")
      log.append(LogCoords(2, 1), "first", "second", "third")
      log.append(LogCoords(3, 4), "fourth", "fifth")

      Then("we should see the entries starting at index 2")
      eventually {
        beforeList should contain only (//
        LogCoords(2, 2) -> "second", //
        LogCoords(2, 3) -> "third", //
        LogCoords(3, 4) -> "fourth" //
        )
      }

      When("We now make another subscription from index 2, as the entries already exist in the log")
      // the entries already exist - now
      val afterList = ListBuffer[(LogCoords, String)]()
      log.appendedEntriesFrom(2).take(3).foreach { after => //
        afterList += after
      }

      Then("We should get the same results as when we observed prior to appending entries")
      eventually {
        afterList.toList shouldBe beforeList.toList
      }
    }
  }
  "ObservableLog.committedEntries and appendedEntries" should {
    "notify observers when an entry is committed" in {
      Given("An observable log with an observer")
      val ol = RaftLog.inMemory[String]().asObservable

      val appendedList  = ListBuffer[(LogCoords, String)]()
      val committedList = ListBuffer[(LogCoords, String)]()
      ol.committedEntries.foreach { msg => //
        committedList += msg
      }
      ol.appendedEntries().foreach { msg => //
        appendedList += msg
      }

      When("three initial entries are appended")
      ol.append(LogCoords(2, 1), "first", "second", "third")

      Then("The observer should be notified of a LogAppendResult")
      eventually {
        appendedList should contain only (LogCoords(2, 3) -> "third", LogCoords(2, 2) -> "second", LogCoords(2, 1) -> "first")
        committedList shouldBe empty
      }

      When("the first entry is committed")
      ol.commit(1)

      Then("The observer should receive a commit message")
      eventually {
        appendedList should contain only (//
        LogCoords(2, 3) -> "third", //
        LogCoords(2, 2) -> "second", //
        LogCoords(2, 1) -> "first" //
        )
        committedList should contain only (LogCoords(2, 1) -> "first")
      }

      When("Another entry is appended in another term")
      ol.append(LogCoords(10, 4), "first entry in term 10")

      Then("The observer should be notified of a LogAppendResult")
      eventually {
        appendedList should contain only (
          LogCoords(10, 4) -> "first entry in term 10", //
          LogCoords(2, 3)  -> "third", //
          LogCoords(2, 2)  -> "second", //
          LogCoords(2, 1)  -> "first" //
        )
        committedList should contain only (LogCoords(2, 1) -> "first")
      }

      When("the remaining entries are committed")
      ol.commit(4)

      Then("The observer should receive a commit message")
      eventually {
        committedList should contain only (
          LogCoords(10, 4) -> "first entry in term 10", //
          LogCoords(2, 3)  -> "third", //
          LogCoords(2, 2)  -> "second", //
          LogCoords(2, 1)  -> "first" //
        )
        appendedList should contain only (
          LogCoords(10, 4) -> "first entry in term 10", //
          LogCoords(2, 3)  -> "third", //
          LogCoords(2, 2)  -> "second", //
          LogCoords(2, 1)  -> "first" //
        )
      }
    }
  }

  "ObservableLog.appendedEntries" should {
    "notify observers when an entry is appended" in {
      val ol = ObservableLog(RaftLog.inMemory[String]())

      val listTask = ol.appendedEntries.take(1).toListL

      ol.append(LogCoords(2, 1), "first commit")

      eventually {
        listTask.runSyncUnsafe(testTimeout) shouldBe List(LogCoords(2, 1) -> "first commit")
      }
    }
    "notify observers when multiple entries are appended" in {
      val ol = RaftLog.inMemory[String]().asObservable

      val received = ListBuffer[(LogCoords, String)]()
      ol.appendedEntries.foreach { msg => //
        received += msg
      }

      val result1: LogAppendResult = ol.append(LogCoords(2, 1), "first", "second", "third")
      result1 shouldBe LogAppendResult(firstIndex = LogCoords(2, 1), lastIndex = LogCoords(2, 3), Nil)
      ol.latestAppended() shouldBe LogCoords(2, 3)

      val result2: LogAppendResult = ol.append(LogCoords(3, 2), "replaced second", "replaced third", "new fourth")
      result2 shouldBe LogAppendResult(firstIndex = LogCoords(3, 2),
                                       lastIndex = LogCoords(3, 4),
                                       replacedIndices = Seq(2, 3))

      eventually {
        received.toList should contain inOrderOnly (//
        LogCoords(2, 1) -> "first", //
        LogCoords(2, 2) -> "second", //
        LogCoords(2, 3) -> "third", //
        LogCoords(3, 2) -> "replaced second", //
        LogCoords(3, 3) -> "replaced third", //
        LogCoords(3, 4) -> "new fourth" //
        )
      }
    }
  }
}
