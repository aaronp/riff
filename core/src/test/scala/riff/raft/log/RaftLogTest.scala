package riff.raft.log

import riff.RiffSpec

class RaftLogTest extends RiffSpec {

  "RaftLog commit" should {
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

  "RaftLog appendAll" should {
    "increment the index for every value its appending" in {
      withLog { log =>
        val res = log.append(LogCoords(2, 1), "1", "two", "three")
        res.replacedIndices shouldBe empty
        res.firstIndex shouldBe 1
        res.lastIndex shouldBe 3
        res.term shouldBe 2

        log.latestAppended() shouldBe LogCoords(2, 3)
        log.latestCommit() shouldBe 0

        log.commit(2) shouldBe Seq(LogCoords(2, 1), LogCoords(2, 2))

        log.latestCommit() shouldBe 2
        log.latestAppended() shouldBe LogCoords(2, 3)
      }
    }
    "error if an attempt is made to skip entries in the log" in {
      withLog { log =>
        val res = log.append(LogCoords(2, 1), "1", "two", "three")
        log.latestAppended() shouldBe LogCoords(2, 3)

        // skip entry 2, go to three
        val bang = intercept[IllegalArgumentException] {
          log.append(LogCoords(2, 3), "skipping entry 2 should fail")
        }

        bang.getMessage should include(
          "Attempt to append LogCoords(2,3) when our latest term is LogCoords(2,3). If an election took place after we were the leader, the term should've been incremented")
      }
    }
    "error if an attempt is made to overwrite a committed index" in {
      withLog { log =>
        val res = log.append(LogCoords(2, 1), "1", "two", "three")
        log.latestAppended() shouldBe LogCoords(2, 3)

        log.commit(2)

        // skip entry 2, go to three
        val bang = intercept[Exception] {
          log.append(LogCoords(3, 2), "trying to overwrite index 2 and 3")
        }

        bang.getMessage should include("Attempt to append LogCoords(3,2) when the latest committed index is 2")
      }

    }

    "replace all entries even if they are not overwritten if a new append contradicts them" in {
      withLog { log =>
        val actual = log.append(LogCoords(2, 1), "first", "second", "third")

        actual shouldBe LogAppendResult(term = 2, firstIndex = 1, lastIndex = 3, Nil)

        log.latestAppended() shouldBe LogCoords(2, 3)

        log.append(LogCoords(3, 1), "new first") shouldBe LogAppendResult(term = 3, firstIndex = 1, lastIndex = 1, Seq(1, 2, 3))

        log.latestAppended() shouldBe LogCoords(3, 1)
        log.latestCommit() shouldBe 0

        log.commit(1) shouldBe Seq(LogCoords(3, 1))
      }
    }
    "replace entries appended as if a new leader overrides uncommitted entries" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first") shouldBe LogAppendResult(term = 2, firstIndex = 1, lastIndex = 1, Nil)

        log.latestAppended() shouldBe LogCoords(2, 1)

        log.append(LogCoords(3, 1), "new first", "two", "three") shouldBe LogAppendResult(term = 3, firstIndex = 1, lastIndex = 3, Seq(1))

        log.latestAppended() shouldBe LogCoords(3, 3)
        log.latestCommit() shouldBe 0

        log.commit(2) shouldBe Seq(LogCoords(3, 1), LogCoords(3, 2))
      }
    }
  }
  "RaftLog append" should {
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

        log.append(LogCoords(7, 1), "first")
        val exp = intercept[Exception] {
          log.append(LogCoords(7, 3), "bang")
        }
        exp.getMessage should include("Attempt to skip a log entry by appending LogCoords(7,3) when the latest entry was LogCoords(7,1)")
      }
    }
    "error if asked to append the same entry with the same term" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(7, 1), "first")
        val exp = intercept[Exception] {
          log.append(LogCoords(7, 1), "bang")
        }
        exp.getMessage should include("Attempt to append LogCoords(7,1) when our latest term is LogCoords(7,1)")
      }
    }

    "error if asked to append the same entry with an earlier term" in {
      withLog { log =>
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(7, 1), "first")
        val exp = intercept[IllegalArgumentException] {
          log.append(LogCoords(6, 1), "bang")
        }
        exp.getMessage should include("Attempt to append LogCoords(6,1) when our latest term is LogCoords(7,1)")

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

  "RaftLog.ForDir append" should {
    "remove old appended entries if asked to append an earlier entry with a greater term" in {
      withDir { dir =>
        val log = RaftLog[String](dir)
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(4, 1), "a") //shouldBe LogAppendResult(log.dir.resolve("1.entry"))
        log.append(LogCoords(4, 2), "b") //shouldBe LogAppendResult(log.dir.resolve("2.entry"))
        log.append(LogCoords(4, 3), "c") //shouldBe LogAppendResult(log.dir.resolve("3.entry"))

        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(4)
        log.termForIndex(3) shouldBe Some(4)
        log.dir.children should contain allElementsOf ((1 to 3).map(i => log.dir.resolve(s"$i.entry")))
        log.latestAppended() shouldBe LogCoords(4, 3)

        // call the method under test -- appending term 5 at index 2 should remove our previous entries
        log.append(LogCoords(5, 2), "replacing entry") shouldBe LogAppendResult(
          term = 5,
          firstIndex = 2,
          lastIndex = 2,
          replacedIndices = Seq(2, 3)
        )
        log.latestAppended() shouldBe LogCoords(5, 2)
        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(5)
        log.termForIndex(3) shouldBe None
      }
    }
  }

  private def withLog(test: RaftLog[String] => Unit): Unit = withDir { dir =>
    withClue("file based") {
      test(RaftLog[String](dir))
    }

    withClue("in memory") {
      test(RaftLog.inMemory[String]())
    }
    withClue("cached") {
      test(RaftLog.inMemory[String]().cached())
    }
  }

}
