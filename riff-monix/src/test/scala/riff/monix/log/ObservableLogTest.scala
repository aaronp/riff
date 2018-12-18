package riff.monix.log
import monix.reactive.Observable
import riff.monix.RiffMonixSpec
import riff.raft.log.{LogAppendResult, LogCoords, RaftLog}
import ObservableLog._
import scala.collection.mutable.ListBuffer

class ObservableLogTest extends RiffMonixSpec {

  "ObservableLog.committedEntriesFrom" should {
    "not feed entries until they are committed" in {
      withScheduler { implicit scheduler =>
        val log                                        = RaftLog.inMemory[String]().observable
        val committed: Observable[(LogCoords, String)] = log.committedEntriesFrom(0)

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
  }
  "ObservableLog.committedCoordsFrom" should {
    "read from events starting in the future when given an index > the current max appended" in {
      withScheduler { implicit scheduler =>
        val log = RaftLog.inMemory[String]().observable

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
    }
    "read from past events" in {
      withScheduler { implicit scheduler =>
        Given("A observable log")
        val log = RaftLog.inMemory[String]().observable

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
  }

  "ObservableLog.appendedEntriesFrom" should {
    "read from events starting in the future when given an index > the current max appended" in {

      withScheduler { implicit scheduler =>
        val log = RaftLog.inMemory[String]().observable

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
    }
    "read from past events" in {

      withScheduler { implicit scheduler =>
        Given("A observable log")
        val log = RaftLog.inMemory[String]().observable

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
  }
  "ObservableLog.committedEntries and appendedEntries" should {
    "notify observers when an entry is committed" in {

      withScheduler { implicit scheduler =>
        Given("An observable log with an observer")
        val ol = RaftLog.inMemory[String]().observable

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
  }

  "ObservableLog.appendedEntries" should {
    "notify observers when an entry is appended" in {
      withScheduler { implicit scheduler =>
        val ol = ObservableLog(RaftLog.inMemory[String]())

        val listTask = ol.appendedEntries.take(1).toListL

        ol.append(LogCoords(2, 1), "first commit")

        eventually {
          listTask.runSyncUnsafe(testTimeout) shouldBe List(LogCoords(2, 1) -> "first commit")
        }
      }
    }
    "notify observers when multiple entries are appended" in {

      withScheduler { implicit scheduler =>
        val ol = RaftLog.inMemory[String]().observable

        val received = ListBuffer[(LogCoords, String)]()
        ol.appendedEntries.foreach { msg => //
          received += msg
        }

        val result1: LogAppendResult = ol.append(LogCoords(2, 1), "first", "second", "third")
        result1 shouldBe LogAppendResult(firstIndex = LogCoords(2, 1), lastIndex = LogCoords(2, 3), Nil)
        ol.latestAppended() shouldBe LogCoords(2, 3)
        eventually {
          // we should always get this value
          received should contain(LogCoords(2, 3) -> "third")
        }

        val result2: LogAppendResult = ol.append(LogCoords(3, 2), "replaced second", "replaced third", "new fourth")
        result2 shouldBe LogAppendResult(firstIndex = LogCoords(3, 2), lastIndex = LogCoords(3, 4), replacedIndices = Seq(LogCoords(2, 2), LogCoords(2, 3)))

        /**
          * This is an interesting assertion of how we got here. This used to be:
          *
          * {{{
          *               received.toList should contain inOrderOnly (//
          *               LogCoords(2, 1) -> "first", //
          *               LogCoords(2, 2) -> "second", //
          *               LogCoords(2, 3) -> "third", //
          *               LogCoords(3, 2) -> "replaced second", //
          *               LogCoords(3, 3) -> "replaced third", //
          *               LogCoords(3, 4) -> "new fourth" //
          *             )
          * }}}
          *
          * which occasionally failed, which it turned out to be from a race condition where we would implement finding
          * and entry by its coordinates (term and index) by looking it up just by the index w/o checking the term still
          * matched the key's coords.
          *
          * So in this test, the observable would see a log entry for e.g. LogCoords(2,2), then ask for the log entry
          * with index 2, but that would return the "replaced second" due to it having been replaced while the observer
          * was doing its thing, and we would just pair the key (LogCoords(2,2)) with the value "replaced second", which
          * was WRONG.
          *
          * A fix was introduced to verify the log entry's term DOES in fact match the term of the LogCoords we were asking
          * for (and if not, just return an empty observable), as that value really doesn't exist any more.
          *
          * We *could* implement soft deletes where we keep the replaced entries, but for now we just do the correct thing
          * and skip that replaced value.
          */
        eventually {
          // we should always get this value
          received should contain(LogCoords(3, 4) -> "new fourth")
        }

        // if we received the last entry (new fourth), then we should always get these values too
        received should contain(LogCoords(2, 1) -> "first")
        received should contain(LogCoords(3, 2) -> "replaced second")
        received should contain(LogCoords(3, 3) -> "replaced third")

        // see comment above - we may not get the second and third, but we CERTAINLY shouldn't get a mix of
        // the second and third coordinates w/ the replaced values
        received should not contain (LogCoords(2, 2) -> "replaced second")
        received should not contain (LogCoords(2, 3) -> "replaced third")
      }
    }
  }
}
