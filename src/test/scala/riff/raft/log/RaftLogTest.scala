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
    "error when trying to commit an entry <= the current commit index" in {
      withLog { log =>
        log.append(LogCoords(2, 1), "first")
        log.append(LogCoords(2, 2), "second")
        val entries = log.commit(2) // commit both
        entries should contain inOrderOnly (LogCoords(2, 1), LogCoords(2, 2))

        val exp = intercept[Exception] {
          log.commit(1)
        }
        exp.getMessage should include("asked to commit 1, but latest committed is 2")
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

        log.append(LogCoords(4, 1), "a") shouldBe LogAppendResult(log.dir.resolve("1.entry"))
        log.append(LogCoords(4, 2), "b") shouldBe LogAppendResult(log.dir.resolve("2.entry"))
        log.append(LogCoords(4, 3), "c") shouldBe LogAppendResult(log.dir.resolve("3.entry"))

        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(4)
        log.termForIndex(3) shouldBe Some(4)
        log.dir.children should contain allElementsOf ((1 to 3).map(i => log.dir.resolve(s"$i.entry")))
        log.latestAppended() shouldBe LogCoords(4, 3)

        // call the method under test -- appending term 5 at index 2 should remove our previous entries
        log.append(LogCoords(5, 2), "replacing entry") shouldBe LogAppendResult(
          written = log.dir.resolve("2.entry"),
          replaced = Seq(
            log.dir.resolve("2.entry"),
            log.dir.resolve("3.entry")
          )
        )
        log.latestAppended() shouldBe LogCoords(5, 2)
        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(5)
        log.termForIndex(3) shouldBe None
      }
    }
  }

  def withLog(test: RaftLog[String] => Unit) = withDir { dir =>
    withClue("file based") {
      test(RaftLog[String](dir))
    }

    withClue("in memory") {
      test(RaftLog.inMemory[String]())
    }
  }

}
