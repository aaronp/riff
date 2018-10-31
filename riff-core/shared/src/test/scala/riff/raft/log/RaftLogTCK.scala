package riff.raft.log
import riff.RiffSpec
import riff.raft.AttemptToOverwriteACommittedIndex

trait RaftLogTCK extends RiffSpec {

  "RaftLog.entriesFrom" should {
    "return entries from a one-based index" in {
      withLog { log =>
        log.appendAll(1, Array(LogEntry(9, "foo"), LogEntry(9, "second")))
        log.entriesFrom(0, 1) shouldBe Array(LogEntry(9, "foo"))
        log.entriesFrom(0, 2) shouldBe Array(LogEntry(9, "foo"), LogEntry(9, "second"))
        log.entriesFrom(1, 1) shouldBe Array(LogEntry(9, "foo"))
        log.entriesFrom(1, 2) shouldBe Array(LogEntry(9, "foo"), LogEntry(9, "second"))
        log.entriesFrom(2, 1) shouldBe Array(LogEntry(9, "second"))
        log.entriesFrom(2, 0) shouldBe empty
        log.entriesFrom(3, 1) shouldBe empty
      }
    }
  }
  "RaftLog.entryForIndex" should {
    "return None when empty" in {
      withLog { log =>
        log.entryForIndex(0) shouldBe None
        log.entryForIndex(1) shouldBe None
        log.entryForIndex(Int.MaxValue) shouldBe None
      }
    }

    "return an entry for one-based index" in {
      withLog { log =>
        log.appendAll(1, Array(LogEntry(1, "foo"), LogEntry(1, "second")))
        log.entryForIndex(0) shouldBe None
        log.entryForIndex(1) shouldBe Some(LogEntry(1, "foo"))
        log.entryForIndex(2) shouldBe Some(LogEntry(1, "second"))
      }
    }
  }

  "RaftLog.commit" should {
    "only commit the first time when given a log index and return an empty list thereafter" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first")
        log.append(LogCoords(2, 2), "second")
        log.append(LogCoords(3, 3), "third")
        log.commit(2) shouldBe Seq(LogCoords(2, 1), LogCoords(2, 2))
        log.commit(2) shouldBe empty
      }
    }
    "error when trying to commit when no entries are appended" in {
      withLog { log =>
        val exp = intercept[Exception] {
          log.commit(1)
        }
        exp.getMessage should include("couldn't find the term for 1")
      }
    }
    "not commit an entry <= the current commit index" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first")
        log.append(LogCoords(2, 2), "second")
        val entries = log.commit(2) // commit both
        entries should contain inOrderOnly (LogCoords(2, 1), LogCoords(2, 2))
        log.commit(1) shouldBe empty
      }
    }
    "return the coords for all committed entries when commit is called" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first")
        log.append(LogCoords(2, 2), "second")
        log.append(LogCoords(3, 3), "third")
        val entries = log.commit(2) // commit both
        entries should contain inOrderOnly (LogCoords(2, 1), LogCoords(2, 2))

        log.commit(3) should contain only (LogCoords(3, 3))
      }
    }
  }

  "RaftLog append" should {
    "overwrite the first entry if another append comes w/ a later term" in {
      withLog { log =>
        log.append(LogCoords(1, 1), "unreplicated 1", "unreplicated 2") shouldBe LogAppendResult(LogCoords(1, 1), LogCoords(1, 2))
        log.append(LogCoords(2, 1), "replaced") shouldBe LogAppendResult(LogCoords(2, 1), LogCoords(2, 1), List(1, 2))
      }
    }
    "not overwrite the first entry if another append comes w/ an earlier term" in {
      withLog { log =>
        log.append(LogCoords(10, 1), "unreplicated 1", "unreplicated 2") shouldBe LogAppendResult(LogCoords(10, 1), LogCoords(10, 2))
        intercept[Exception] {
          log.append(LogCoords(9, 1), "replaced") shouldBe LogAppendResult(LogCoords.Empty, LogCoords.Empty)
        }
        log.entryForIndex(1) shouldBe Some(LogEntry(10, "unreplicated 1"))
        log.entryForIndex(2) shouldBe Some(LogEntry(10, "unreplicated 2"))
      }
    }
    "increment the index for every value its appending" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "1", "two", "three") shouldBe LogAppendSuccess(LogCoords(2, 1), LogCoords(2, 3))

        log.latestAppended() shouldBe LogCoords(2, 3)
        log.latestCommit() shouldBe 0

        log.commit(2) shouldBe Seq(LogCoords(2, 1), LogCoords(2, 2))

        log.latestCommit() shouldBe 2
        log.latestAppended() shouldBe LogCoords(2, 3)
      }
    }
    "error if an attempt is made to skip entries in the log" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "1", "two", "three")
        log.latestAppended() shouldBe LogCoords(2, 3)

        // skip entry 2, go to three
        log.append(LogCoords(3, 5), "skipping entry 4 should fail") shouldBe AttemptToSkipLogEntry(LogCoords(3, 5), 4)
      }
    }
    "error if an attempt is made to overwrite a committed index" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "1", "two", "three")
        log.latestAppended() shouldBe LogCoords(2, 3)

        log.commit(3)

        // try and append to index 3, which has been committed
        val bang = intercept[AttemptToOverwriteACommittedIndex] {
          log.append(LogCoords(10, 3), "trying to overwrite index 3, term 10")
        }

        bang shouldBe AttemptToOverwriteACommittedIndex(3, 3)
      }

    }

    "replace all entries even if they are not overwritten if a new append contradicts them" in {
      withLog { log =>
        val actual = log.append(LogCoords(2, 1), "first", "second", "third")

        actual shouldBe LogAppendResult(firstIndex = LogCoords(2, 1), lastIndex = LogCoords(2, 3), Nil)

        log.latestAppended() shouldBe LogCoords(2, 3)

        log.append(LogCoords(3, 1), "new first") shouldBe LogAppendResult(firstIndex = LogCoords(3, 1), lastIndex = LogCoords(3, 1), Seq(1, 2, 3))

        log.latestAppended() shouldBe LogCoords(3, 1)
        log.latestCommit() shouldBe 0

        log.commit(1) shouldBe Seq(LogCoords(3, 1))
      }
    }
    "replace entries appended as if a new leader overrides uncommitted entries" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first") shouldBe LogAppendResult(firstIndex = LogCoords(2, 1), lastIndex = LogCoords(2, 1), Nil)

        log.latestAppended() shouldBe LogCoords(2, 1)

        log.append(LogCoords(3, 1), "new first", "two", "three") shouldBe LogAppendResult(firstIndex = LogCoords(3, 1), lastIndex = LogCoords(3, 3), Seq(1))

        log.latestAppended() shouldBe LogCoords(3, 3)
        log.latestCommit() shouldBe 0

        log.commit(2) shouldBe Seq(LogCoords(3, 1), LogCoords(3, 2))
      }
    }

    "remove old appended entries if asked to append an earlier entry with a greater term" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(4, 1), "a")
        log.append(LogCoords(4, 2), "b")
        log.append(LogCoords(4, 3), "c")

        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(4)
        log.termForIndex(3) shouldBe Some(4)
        log.latestAppended() shouldBe LogCoords(4, 3)

        // call the method under test -- appending term 5 at index 2 should remove our previous entries
        log.append(LogCoords(5, 2), "replacing entry")

        log.latestAppended() shouldBe LogCoords(5, 2)
        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(5)
        log.termForIndex(3) shouldBe None
      }
    }
    "error if asked to skip a log entry" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(7, 1), "first") shouldBe LogAppendResult(LogCoords(7, 1), LogCoords(7, 1))
        log.append(LogCoords(7, 3), "bang") shouldBe AttemptToSkipLogEntry(LogCoords(7, 3), 2)
      }
    }
    "error if asked to append the same entry with the same term" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(7, 1), "first") shouldBe LogAppendResult(LogCoords(7, 1), LogCoords(7, 1))
        log.append(LogCoords(7, 1), "bang") shouldBe AttemptToAppendLogEntryAtEarlierTerm(LogCoords(7, 1), LogCoords(7, 1))
      }
    }

    "error if asked to append the same entry with an earlier term" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(7, 1), "first") shouldBe LogAppendResult(LogCoords(7, 1), LogCoords(7, 1))
        log.append(LogCoords(6, 1), "bang") shouldBe AttemptToAppendLogEntryAtEarlierTerm(LogCoords(6, 1), LogCoords(7, 1))
      }
    }
    "increment the index on each append" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.termForIndex(0) shouldBe None
        log.termForIndex(1) shouldBe None
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(2, 1), "first entry")
        log.logState shouldBe LogState(commitIndex = 0, latestTerm = 2, latestIndex = 1)
        log.termForIndex(1) shouldBe Some(2)

        log.append(LogCoords(2, 2), "first entry")
        log.logState shouldBe LogState(commitIndex = 0, latestTerm = 2, latestIndex = 2)
        log.termForIndex(2) shouldBe Some(2)

        log.append(LogCoords(3, 3), "first entry")
        log.logState shouldBe LogState(commitIndex = 0, latestTerm = 3, latestIndex = 3)
        log.termForIndex(2) shouldBe Some(2)
        log.termForIndex(3) shouldBe Some(3)

        log.append(LogCoords(3, 4), "first entry")
        log.logState shouldBe LogState(commitIndex = 0, latestTerm = 3, latestIndex = 4)
        log.termForIndex(4) shouldBe Some(3)
      }
    }
  }

  protected def withLog(test: RaftLog[String] => Unit): Unit
}
