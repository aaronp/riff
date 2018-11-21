package riff.monix.client

import monix.execution.Scheduler
import monix.reactive.Observer
import monix.reactive.subjects.Var
import riff.monix.RiffMonixSpec
import riff.raft.log._
import riff.raft.messages._
import riff.raft.{AppendStatus, LogIndex, NodeId, Term}

import scala.collection.mutable.ListBuffer

class AppendStatusObservableTest extends RiffMonixSpec {

  // These scenarios are for handling log error events
  //
  // we continue to monitor log append messages in the response stream as a means to
  // check whether our append gets "rolled back" due to it having been received by a disconnected/stale
  // leader.
  //
  // e.g. the scenario where
  // 1) we append some data to the leader of a 5 node cluster
  // 2) the leader is disconnected from the cluster before that append is replicated to all nodes
  //    In a 5 node cluster, it could be event replicated to 1 other node but unreachable by the other 3
  // 3) another node in the 3-node cluster becomes the leader. At the point we can't yet rollback/error,
  //    as our disconnected leader could _still_ time-out, request a vote, and become leader again
  // 4) The old, stale leader receives an append request from some new data sent from the new leader, thus
  //    over-writing the data from step #1. At this point our log will replace the data in #1, and it is this
  //    we're listening to in order to know whether we should error the client's stream.
  //
  // this test is for when that log stream produces an error. It could be that we might want to ignore errors, but
  // in the first instance I think we should fail-fast/early until it proves we need to reconsider.
  "SingleAppendFSM.prepareAppendFeed given log errors" should {

    "complete in error when an initial LogAppendResult in error is sent" in {
      withScheduler { implicit s =>
        Given("A status feed")
        val scenario = new TestScenario()

        And("The feed is given a log append result in error")
        val expectedErr = AttemptToSkipLogEntry(LogCoords(2, 3), 2)
        scenario.sendInitialLogResult(expectedErr)

        Then("It should complete the stream w/ that error")
        val err = eventually {
          scenario.errorOpt.get
        }
        err shouldBe expectedErr
      }
    }

    // even though this node could become the leader again, presumably we want the observable given to clients to
    // fail fast so that they have the information needed to know they could/should try to recover by sending the
    // data to the new leader
    List(
      NotTheLeaderException("some id", 2, None),
      AttemptToSkipLogEntry(LogCoords(2, 3), 2),
      AttemptToAppendLogEntryAtEarlierTerm(LogCoords.Empty, LogCoords.Empty)
    ).foreach { expectedErr =>
      s"complete in error after a ${expectedErr.getClass.getSimpleName} is encountered" in {
        withScheduler { implicit s =>
          Given("A status feed")
          val scenario = new TestScenario()

          And("The feed is given an initial successful append response")
          scenario.sendInitialLogResult(LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 3)))

          When(s"The log feed is given $expectedErr")
          scenario.sendLogResult(expectedErr)

          Then("The client stream should complete in error")
          eventually {
            scenario.completed shouldBe true
          }

          scenario.received.last.errorAfterAppend shouldBe Some(expectedErr)
        }
      }
    }
  }

  "SingleAppendFSM.prepareAppendFeed" should {

//    "publish a committed status when the log is committed" in {
//      ???
//    }
//    "publish a committed status if it observes a commit log message for a log coordinate AFTER the appended coordinate" in {
//      ???
//    }
//    "not publish a committed status when it observes committed messages for unrelated coordinates" in {
//      ???
//
//    }
    "publish all committed events sent prior to subscription" in {
      withScheduler { implicit s =>
        Given("A status feed which does NOT initially subscribe to the data")
        val scenario = new TestScenario(clusterSize = 3, subscribeToFeed = false)

        And("The feed is given the result of a log append with multiple log entries")
        scenario.sendInitialLogResult(LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 10)))

        When("The node receives append responses for some of the entries, as if they were sent piecemeal from the leader")
        scenario.sendAppendEntriesResponse("follower 1", 3, term = 2)
        scenario.sendAppendEntriesResponse("follower 1", 4, term = 2)
        scenario.sendAppendEntriesResponse("follower 1", 5, term = 2)
        scenario.sendAppendEntriesResponse("follower two", 9, term = 2)

        And("The node receives a few commit messages")
        scenario.sendCommitMsg(LogCoords(2, 3))
        scenario.sendCommitMsg(LogCoords(2, 4))
        scenario.sendCommitMsg(LogCoords(2, 5))

        When("The feed is finally subscribed to")
        scenario.addSubscription()

        Then("It should receive a status which contains all the committed log coordinates")
        eventually {
          scenario.received.map(_.committed) should contain allOf (Set(LogCoords(2, 3)), //
          Set(LogCoords(2, 3), LogCoords(2, 4)), //
          Set(LogCoords(2, 3), LogCoords(2, 4), LogCoords(2, 5)))
        }
      }
    }
    "publish all events sent by the nodeInput prior to subscription" in {
      withScheduler { implicit s =>
        Given("A status feed which does NOT initially subscribe to the data")
        val scenario = new TestScenario(clusterSize = 3, subscribeToFeed = false)

        And("The feed is given the result of a log append")
        scenario.sendInitialLogResult(LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 3)))

        When("The node receives all its append responses")
        scenario.sendAppendEntriesResponse("follower 1", 3, term = 2)
        scenario.sendAppendEntriesResponse("follower 2", 3, term = 2)

        And("The node receives a commit message")
        scenario.sendCommitMsg(LogCoords(2, 3))

        When("The feed is finally subscribed to")
        scenario.addSubscription()

        Then("It should receive a completed list of events")
        eventually {
          scenario.completed shouldBe true
        }

        scenario.received should contain(
          AppendStatus(
            leaderAppendResult = LogAppendSuccess(LogCoords(term = 2, index = 3), LogCoords(term = 2, index = 3)),
            appendedCoords = Set(LogCoords(term = 2, index = 3)),
            appended = Map("the node id" -> AppendEntriesResponse(2, true, 3), //
                           "follower 1"  -> AppendEntriesResponse(2, true, 3), //
                           "follower 2"  -> AppendEntriesResponse(2, true, 3)),
            clusterSize = 3,
            committed = Set(LogCoords(term = 2, index = 3))
          ))
      }
    }
    "publish updates when either the nodeInput or appendResult change" in {
      withScheduler { implicit s =>
        Given("A status feed")
        val scenario = new TestScenario()

        And("The feed is given the result of a log append")
        val firstStatus = scenario.expectStatusUpdate {
          scenario.sendInitialLogResult(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 3)))
        }
        firstStatus shouldBe AppendStatus(
          leaderAppendResult = LogAppendSuccess(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 3), List()),
          clusterSize = 7,
          appendedCoords = Set(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 2), LogCoords(term = 1, index = 3)),
          appended = Map("the node id" -> AppendEntriesResponse.ok(1, 3)),
          committed = Set()
        )

        When("The node receives a matching AppendEntriesResponse")
        val secondStatus = scenario.expectStatusUpdate {
          scenario.sendAppendEntriesResponse("some node", 1)
        }

        Then("It should produce an AppendStatus message")
        secondStatus shouldBe AppendStatus(
          leaderAppendResult = LogAppendSuccess(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 3), List()),
          clusterSize = 7,
          appendedCoords = Set(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 2), LogCoords(term = 1, index = 3)),
          appended = Map("the node id" -> AppendEntriesResponse.ok(1, 3), "some node" -> AppendEntriesResponse.ok(1, 1)),
          committed = Set()
        )
      }
    }
    "not publish updates when an AppendEntriesResponse is received for a different log coordinate" in {
      withScheduler { implicit s =>
        Given("A status feed")
        val scenario = new TestScenario()

        And("The feed is given the result of a log append")
        val firstStatus = scenario.expectStatusUpdate {
          scenario.sendInitialLogResult(LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 3)))
        }
        firstStatus shouldBe AppendStatus(
          leaderAppendResult = LogAppendSuccess(LogCoords(2, 3), LogCoords(2, 3), List()),
          clusterSize = 7,
          appendedCoords = Set(LogCoords(term = 2, 3)),
          appended = Map("the node id" -> AppendEntriesResponse.ok(2, 3)),
          committed = Set()
        )

        scenario.appendStatusMessagesReceivedCount shouldBe 1
        When("The node receives a non-matching AppendEntriesResponse")
        scenario.sendAppendEntriesResponse("some node", 2)

        Then("It should NOT receive an updated message")
        Thread.sleep(testNegativeTimeout.toMillis)
        scenario.appendStatusMessagesReceivedCount shouldBe 1
      }
    }
  }

  class TestScenario(nodeId: NodeId = "the node id", clusterSize: Int = 7, maxCapacity: Int = 10, subscribeToFeed: Boolean = true)(implicit sched: Scheduler) {
    val inputToNode   = Var[RaftMessage[String]](ReceiveHeartbeatTimeout)
    val logFeed       = Var[LogAppendResult](null)
    val committedFeed = Var[LogCoords](null)

    var errorOpt            = Option.empty[Throwable]
    val received            = ListBuffer[AppendStatus]()
    @volatile var completed = false

    val (appendResultObj, feed) = {

      // the 'prepareAppendFeed' is the function under test
      val (input, stream) = AppendStatusObservable(
        nodeId,
        clusterSize = clusterSize,
        inputToNode,
        logFeed.filter(_ != null),
        committedFeed.filter(_ != null),
        maxCapacity
      )

      input -> stream
        .doOnError { err =>
          errorOpt = Option(err)
        }
        .doOnComplete(() => completed = true)
    }

    // we allow some test scenarios to delay this until after some events are sent
    if (subscribeToFeed) {
      addSubscription()
    }

    def addSubscription(): Unit = {
      feed.foreach { next =>
        received += next
      }
    }

    def appendStatusMessagesReceivedCount() = received.size

    def expectStatusUpdate(f: => Unit): AppendStatus = {
      expectStatusUpdates(1)(f).head
    }

    def expectStatusUpdates(nr: Int)(thunk: => Unit): List[AppendStatus] = {
      val size = received.size
      thunk
      eventually {
        received.size shouldBe size + nr
      }
      received.takeRight(1).toList
    }

    def sendCommitMsg(msg: LogCoords) = {
      committedFeed := msg
    }

    def sendInitialLogResult(result: LogAppendResult) = {
      Observer.feed(appendResultObj, result :: Nil).futureValue

      sendLogResult(result)
    }

    def sendLogResult(result: LogAppendResult) = {
      val fut = logFeed := result
      fut.futureValue
    }

    def sendAppendEntriesResponse(from: String, matchIndex: LogIndex, term: Term = 1) = {
      val fut = inputToNode := AddressedMessage(from, AppendEntriesResponse.ok(term, matchIndex))
      fut.futureValue
    }

    def sendAppendEntriesResponseInError(from: String, matchIndex: LogIndex, term: Term = 1) = {
      val fut = inputToNode := AddressedMessage(from, AppendEntriesResponse.fail(term))
      fut.futureValue
    }

  }
}
