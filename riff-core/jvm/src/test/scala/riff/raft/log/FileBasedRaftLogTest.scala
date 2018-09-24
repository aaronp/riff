package riff.raft.log

class FileBasedRaftLogTest extends RaftLogTCK {

  "RaftLog(file)" should {
    "error if given a file as a path" in {
      withDir { dir =>
        val file = dir.resolve("imAFile.txt").text = "hi"
        val exp = intercept[Exception] {
          RaftLog[String](file)
        }
        exp.getMessage should include("imAFile.txt is not a directory")
      }
    }
    "error if the directory doesn't exist" in {
      withDir { dir =>
        val doesntExist = dir.resolve("data").resolve("instance-1")
        val exp = intercept[Exception] {
          RaftLog[String](doesntExist)
        }
        exp.getMessage should include("is not a directory")
      }
    }
    "create the directory if it doesn't exist and 'createIfNotExists' is specified" in {
      withDir { dir =>
        val doesntExist = dir.resolve("data").resolve("instance-1")
        doesntExist.exists() shouldBe false
        val log = RaftLog[String](doesntExist, createIfNotExists = true)
        doesntExist.exists() shouldBe true
        log.append(LogCoords(4, 5), "hi")
        doesntExist.children should not be empty
      }
    }
  }
  "RaftLog.ForDir append" should {
    "remove old appended entries if asked to append an earlier entry with a greater term" in {
      withDir { dir =>
        val log = RaftLog[String](dir)
        log.logState shouldBe LogState.Empty

        log.append(LogCoords(4, 1), "a")
        log.append(LogCoords(4, 2), "b")
        log.append(LogCoords(4, 3), "c")

        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(4)
        log.termForIndex(3) shouldBe Some(4)
        log.dir.children should contain allElementsOf ((1 to 3).map(i => log.dir.resolve(s"$i.entry")))
        log.latestAppended() shouldBe LogCoords(4, 3)

        // call the method under test -- appending term 5 at index 2 should remove our previous entries
        log.append(LogCoords(5, 2), "replacing entry") shouldBe LogAppendResult(
          firstIndex = LogCoords(5, 2),
          lastIndex = LogCoords(5, 2),
          replacedIndices = Seq(2, 3)
        )
        log.latestAppended() shouldBe LogCoords(5, 2)
        log.termForIndex(1) shouldBe Some(4)
        log.termForIndex(2) shouldBe Some(5)
        log.termForIndex(3) shouldBe None
      }
    }
  }

  override protected def withLog(testLogic: RaftLog[String] => Unit): Unit = {
    withDir { dir =>
      withClue("file based") {
        testLogic(RaftLog[String](dir))
      }
    }

    withDir { dir =>
      withClue("file based cached") {
        testLogic(RaftLog[String](dir).cached())
      }
    }
  }
}
