package riff.monix
import eie.io._
import org.scalatest.concurrent.Eventually
import riff.RiffSpec
import riff.monix.EventSource.StateDao

import riff.raft.LogIndex
import riff.raft.log.{LogCoords, RaftLog}

import scala.collection.mutable.ListBuffer
import scala.compat.Platform
import scala.util.{Success, Try}

class EventSourceTest extends RiffMonixSpec {

  "EventSource" should {
    "subscribe from the point of the last snapshot" in {
      withDir { dataDir =>
        Given("A means of reading/writing our state (a set of strings)")
        implicit val setFromBytes = FromBytes.Utf8String.map(_.lines.toSet)
        implicit val setToBytes = ToBytes.Utf8String.contramap[Set[String]](_.mkString(Platform.EOL))

        And("an observable log of events")
        import riff.monix.log.ObservableLog._
        val log = RaftLog.inMemory[String]().observable

        When("We subscribe to that log with our snapshot utility which writes down the snapshots every 3 updates")
        val dao = EventSource.dao[Set[String], String](dataDir, Set.empty[String])
        val Success(states) = EventSource[Set[String], String](dao, log, 3) { (set, next) => //
          set + next
        }

        // keep track of states as we
        val stateUpdates = ListBuffer[Set[String]]()
        states.foreach { nextState => //  I need to find the scalafmt setting to stop this being in-lined
          stateUpdates += nextState
        }

        Then("the initial state should not yet be written down")
        dataDir.children.toList.map(_.fileName) should contain only (".latestSnapshotIndex")

        When("we send 4 updates through the log")
        (1 to 4).foreach { i =>
          log.append(LogCoords(1, i), s"entry $i")
        }

        Then("We should get 4 state updates when committed")
        stateUpdates shouldBe empty
        dao.latestSnapshot().map(_._1) shouldBe Success(0)
        log.latestAppended() shouldBe LogCoords(1, 4)
        log.commit(4)

        eventually {
          stateUpdates should contain only (
            Set("entry 1"),
            Set("entry 1", "entry 2"),
            Set("entry 1", "entry 2", "entry 3"),
            Set("entry 1", "entry 2", "entry 3", "entry 4")
          )
        }

        And("we should have written down our state and received the 4 updates")
        eventually {
          dataDir.children.toList.map(_.fileName) should contain only (".latestSnapshotIndex", "snapshot-4")
        }
        dao.latestSnapshot().map(_._1) shouldBe Success(4)
        val LogCoords.FromKey(coords) = dataDir.resolve(".latestSnapshotIndex").text
        coords shouldBe LogCoords(1, 4)

        When("we send 4 more updates through the log")
        (5 to 8).foreach { i =>
          log.append(LogCoords(1, i), s"entry $i")
        }

        Then("the uncommitted entries should not have yet been written")
        log.latestAppended() shouldBe LogCoords(1, 8)
        dao.latestSnapshot() shouldBe Success(4 -> (1 to 4).map(i => s"entry $i").toSet)

        When("The latest entries is committed")
        log.commit(8)

        Then("the entries should be received by the state machine and new snapshot should be written")
        val expected7 = (1 to 8).inits.collect {
          case list if list.nonEmpty =>
            list.map(i => s"entry $i").toSet
        }.toList

        eventually {
          dao.latestSnapshot().map(_._1) shouldBe Success(7)
          stateUpdates should contain theSameElementsAs (expected7)
        }
        dataDir.children.toList.map(_.fileName) should contain only (".latestSnapshotIndex", "snapshot-4", "snapshot-7")
        val LogCoords.FromKey(latestSnappedCoords) = dataDir.resolve(".latestSnapshotIndex").text
        latestSnappedCoords shouldBe LogCoords(1, 7)

        When("we try again to observe the log")
        object testDao extends StateDao[Set[String]] {

          val readSnapshots = ListBuffer[(LogIndex, Set[String])]()
          override def latestSnapshot(): Try[(LogIndex, Set[String])] = {
            val read = dao.latestSnapshot()
            readSnapshots += read.get
            read
          }

          override def writeDown(coords: LogCoords, s: Set[String]): Unit = {
            dao.writeDown(coords, s)
          }
        }

        val Success(secondTimeStates) = EventSource[Set[String], String](testDao, log, 3) { (set, next) => //
          set + next
        }

        val finalState: Set[String] = (1 to 8).map(i => s"entry $i").toSet

        withClue(
          "the second time we run the event source, it starts from the latest snapshot, and so should only have 2 updates") {
          secondTimeStates.take(2).toListL.runSyncUnsafe(testTimeout) should contain only (
            finalState - "entry 8",
            finalState
          )
        }

        Then("it should have read from the last snapshot, whose last element was 'entry 7'")
        testDao.readSnapshots.toList shouldBe List((7, finalState - "entry 8"))
      }
    }
  }
}
