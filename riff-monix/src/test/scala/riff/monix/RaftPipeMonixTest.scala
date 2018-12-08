package riff.monix

import monix.eval.Task
import monix.execution.{Ack, Scheduler}
import monix.reactive.subjects.Var
import monix.reactive.{Observable, Observer}
import riff.RaftPipe
import riff.monix.RaftPipeMonix.{onAppendData, pipeForHandler}
import riff.monix.RaftPipeMonixTest.PausablePipe
import riff.monix.log.ObservableLog
import riff.raft._
import riff.raft.log.{LogAppendSuccess, LogCoords, LogEntry, RaftLog}
import riff.raft.messages._
import riff.raft.node.RoleCallback.NewLeaderEvent
import riff.raft.node._
import riff.raft.timer.LoggedInvocationClock
import riff.reactive.{ReactivePipe, TestListener}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class RaftPipeMonixTest extends RiffMonixSpec {

  "RaftPipe.client" should {

    // scenario where a leader is elected in a 5-node cluster, gets disconnected from 3 nodes while accepting
    // some appends and sending to its remaining follower before being told of the new leader amongst the 3 nodes
    "append results should error if the append was to a disconnected leader" in {

      withScheduler { implicit scheduler =>
        // use a clock which never actually sends timeouts
        implicit val clock = new LoggedInvocationClock

        Given("A five node cluster (with a clock which never invokes anything)")
        val fiveNodeCluster: Map[NodeId, PausablePipe[String]] = RaftPipeMonixTest.asPausableCluster((1 to 5).map(i => RaftNode.inMemory[String](s"node-$i")): _*)

        try {
          val initialLeader: PausablePipe[String] = fiveNodeCluster("node-1")
          val leaderEventsTasks: Iterable[Task[NodeId]] = {
            val states: Iterable[ObservableState] = fiveNodeCluster.values.map(_.handler.underlying.roleCallback.asInstanceOf[ObservableState])

            states.map { obsState =>
              val replay = obsState.events.replay
              replay.connect
              replay.collect {
                case NewLeaderEvent(_, leaderId) => leaderId
              }.headL

            }
          }

          //leaderEvents.head.foreach(_ shouldBe node1.nodeId)

          // 1) elect a leader
          And("A leader is elected (after we manually trigger the receive HB timeout)")
          initialLeader.input.onNext(ReceiveHeartbeatTimeout)

          val leaderIds = leaderEventsTasks.map(_.runSyncUnsafe(testTimeout))
          leaderIds.foreach(_ shouldBe initialLeader.nodeId)
          initialLeader.handler.underlying.state().isLeader shouldBe true

          // 2) disconnect all but one node
          And("We then disconnect three nodes from the leader")
          val pausedNodes = Set("node-3", "node-4", "node-5")
          pausedNodes.foreach { id => //
            fiveNodeCluster(id).handler.pause() shouldBe true
          }

          def interceptAppendsFromPausedNodes(): Map[NodeId, AppendEntries[String]] = {

            val interceptedAppends: Set[(NodeId, AppendEntries[String])] = pausedNodes.map { id => //
              val interceptedRequest = eventually {
                fiveNodeCluster(id).handler.pausedHandler.requests().head
              }
              interceptedRequest match {
                case AddressedMessage("node-1", hb: AppendEntries[String]) =>
                  id -> hb
                case other =>
                  fail(s"Expected a heartbeat message from node-1, but got $other")
                  ???
              }
            }
            interceptedAppends.size shouldBe pausedNodes.size
            interceptedAppends.toMap
          }

          initialLeader.input.onNext(SendHeartbeatTimeout)
          interceptAppendsFromPausedNodes().values.foreach(_.entries shouldBe empty)

          // 3) do our append request
          When("An append is received by the current leader")

          val appendUpdates: Observable[AppendStatus] = initialLeader.client.append("this won't get committed")

          // 4) get the 2 response status messages (the leader plus one remaining follower)
          Then("the append response should notify two messages are received")
          appendUpdates.take(2).countL.runSyncUnsafe(testTimeout) shouldBe 2

          And("The paused nodes should have received (but ignored) their append requests")
          // verify we ignored the requests
          interceptAppendsFromPausedNodes().values.foreach(_.entries.map(_.data) should contain only ("this won't get committed"))

          // 5) now reconnect the handler and trigger a new election from node 2
          When("the three nodes are reconnected and triggers an election")
          pausedNodes.foreach { id => //
            fiveNodeCluster(id).handler.resume() shouldBe true
          }
          val newLeader: PausablePipe[String] = fiveNodeCluster("node-3")
          newLeader.input.onNext(ReceiveHeartbeatTimeout)
          val newLeaderNode = newLeader.handler.underlying

          And("node-2 becomes the new leader")
          eventually {
            newLeaderNode.state().isLeader shouldBe true
          }
          val newLeaderTerm = newLeaderNode.currentTerm()
          eventually {
            fiveNodeCluster.values.map(_.handler.underlying.currentTerm()).foreach { term => //
              term shouldBe newLeaderTerm
            }
          }
          And("node-2 appends some data which will overwrites our uncommitted entry from the original leader")
          val clobberAppendResults = newLeader.client.append("It's clobber time!")
          val secondAppendAll      = clobberAppendResults.toListL.runSyncUnsafe(testTimeout)

          Then("the append listener should fail with an 'Im not the leader anymore' error")
          val all = appendUpdates.toListL.runSyncUnsafe(testTimeout)

          all.foreach(_.committed shouldBe empty)
          val caught = all.last.errorAfterAppend.get
          caught shouldBe AppendOccurredOnDisconnectedLeader(LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 1)),
                                                             LogAppendSuccess(LogCoords(2, 1), LogCoords(2, 1), Seq(LogCoords(1, 1))))

          And("All the nodes' logs should be consistent w/ the new entry")
          val logs: List[RaftLog[String]] = fiveNodeCluster.values.map(_.handler.underlying.log).toList
          val expected                    = LogEntry[String](newLeaderTerm, "It's clobber time!")
          eventually {
            logs.foreach { log => //
              log.entriesFrom(1) should contain only (expected)
            }
          }

        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
      }
    }

    "elect a leader in a five node cluster" in {

      withScheduler { implicit scheduler =>
        implicit val clock = newClock

        val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)

        val oneNode = fiveNodeCluster.head._2.handler

        // also observe cluster events
        var leaderEvents: List[NewLeaderEvent] = Nil
        oneNode.roleCallback.asInstanceOf[ObservableState].events.foreach {
          case newLeader: NewLeaderEvent => leaderEvents = newLeader :: leaderEvents
          case _                         =>
        }

        try {
          fiveNodeCluster.values.foreach(_.resetReceiveHeartbeat())

          val leaderNodeAndTerm: (NodeId, Term) = eventually {
            fiveNodeCluster.values.collectFirst {
              case n if n.handler.state().isLeader =>
                n.handler.nodeId -> n.handler.currentTerm()
            }.get
          }

          eventually {
            withClue(s"new leader events ${leaderEvents} and found leader ${leaderNodeAndTerm}") {
              leaderEvents.map(e => (e.leaderId, e.term)) should contain(leaderNodeAndTerm)
            }
          }
        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
      }
    }

    "send notifications even if subscribed to after the entry is committed" in {

      withScheduler { implicit scheduler =>
        implicit val clock = newClock

        val fiveNodeCluster = RaftPipeMonix.inMemoryClusterOf[String](5)
        try {

          val obsLogById: Map[NodeId, ObservableLog[String]] = fiveNodeCluster.map {
            case (nodeId, nodePipe) =>
              val obsLog = nodePipe.handler.log.asInstanceOf[ObservableLog[String]]
              (nodeId, obsLog)
          }
          fiveNodeCluster.values.foreach(_.resetReceiveHeartbeat())

          val leader = eventually {
            fiveNodeCluster.values.find(_.handler.state().isLeader).get
          }

          leader.client.append("input").completedL.runSyncUnsafe(testTimeout)

        } finally {
          fiveNodeCluster.values.foreach(_.close())
        }
      }
    }
  }

  "RaftPipeMonix.publisherFor" should {
    "publisher events destined for a particular node" in {

      withScheduler { implicit scheduler =>
        implicit val clock = newClock
        val node: RaftNode[String] = {
          val n = RaftNode.inMemory[String]("test").withCluster(RaftCluster("first", "second"))
          n.withLog(ObservableLog(n.log))
        }
        val pipeUnderTest: RaftPipe[String, Observer, Observable, Observable, RaftNode[String]] = RaftPipeMonix.raftPipeForHandler(node)

        try {

          import riff.reactive.AsPublisher.syntax._
          val forFirst    = pipeUnderTest.publisherFor("first")
          val firstInputs = forFirst.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

          val forSecond    = pipeUnderTest.publisherFor("second")
          val secondInputs = forSecond.subscribeWith(new TestListener[RaftMessage[String]](10, 100))

          // poke the node under test, forcing it to send messages to the cluster (e.g. first and second nodes)
          val in = pipeUnderTest.input
          in.onNext(ReceiveHeartbeatTimeout)

          eventually {
            val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = firstInputs.received.toList
            term should be > 0
          }
          eventually {
            val List(AddressedMessage("test", RequestVote(term, LogCoords.Empty))) = secondInputs.received.toList
            term should be > 0
          }
        } finally {
          pipeUnderTest.close()
        }
      }
    }
  }

  "RaftPipeMonix.pipeForNode" should {

    "continue to work when one of the input feeds fails" in {

      withScheduler { implicit scheduler =>
        Given("A ReactivePipe whose input subscribes to two feeds")
        implicit val clock = new LoggedInvocationClock // don't actually trigger any heartbeat timeouts - we'll do that manually
        val node           = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest  = RaftPipeMonix.pipeForNode(node)
        val received       = ListBuffer[RaftNodeResult[String]]()
        pipeUnderTest.output.subscribe { msg =>
          received += msg
          Ack.Continue
        }

        When("One of those feeds completes with an error")
        try {

          val erroredInput: Observable[RaftMessage[String]] = Observable(ReceiveHeartbeatTimeout).endWithError(new Exception("some error"))

          val okInput = Var[RaftMessage[String]](null)
          okInput.filter(_ != null).subscribe(pipeUnderTest.input)
          erroredInput.subscribe(pipeUnderTest.input)

          And("The remaining input sends some messages")
          eventually {
            received.size should be > 0
          }

          val beforeTerm = node.persistentState.currentTerm
          Then("they should still be processed")
          okInput := ReceiveHeartbeatTimeout

          eventually {
            node.persistentState.currentTerm should be > beforeTerm
          }
        } finally {
          pipeUnderTest.close()
        }
      }
    }
    "publish the results of the inputs" in {
      withScheduler { implicit scheduler =>
        implicit val clock                                                                                 = newClock
        val node                                                                                           = RaftNode.inMemory[String]("test").withCluster(RaftCluster("peer node A", "pier node B"))
        val pipeUnderTest: ReactivePipe[RaftMessage[String], RaftNodeResult[String], Observer, Observable] = RaftPipeMonix.pipeForNode(node)

        try {

          val listener = pipeUnderTest.subscribeWith(new TestListener[RaftNodeResult[String]](10, 100))
          val pi       = pipeUnderTest.input

          pi.onNext(ReceiveHeartbeatTimeout)

          eventually {
            listener.received.size shouldBe 1
          }

          val requestVote                      = RequestVote(1, LogCoords.Empty)
          val List(AddressedRequest(requests)) = listener.received.toList

          requests should contain only (("pier node B" -> requestVote), ("peer node A" -> requestVote))
        } finally {
          pipeUnderTest.close()
        }
      }
    }
  }

  "RaftPipeMonix" should {

    // failing on travis for complete
    "construct an endpoint which we can used to communicate w/ other endpoints" ignore {

      withScheduler { implicit sched =>
        Given("Two raft nodes linked together")

        implicit val everyonesClock = new LoggedInvocationClock // manually trigger clock events

        val cluster = RaftPipeMonix.asCluster[String](RaftNode.inMemory("a"), RaftNode.inMemory("b"))

        try {
          When("One becomes the leader")
          val List(leader, follower) = cluster.values.toList

          leader.input.onNext(ReceiveHeartbeatTimeout).futureValue
          eventually {
            leader.handler.state().isLeader
            leader.handler.currentTerm() shouldBe follower.handler.currentTerm()
          }

          And("We append some data to that leader")
          val appendResultPublisher: Observable[AppendStatus] = leader.client.append("Hello", "World")

          val received  = ListBuffer[AppendStatus]()
          var completed = false
          appendResultPublisher.doOnComplete(() => completed = true).foreach { status => //
            received += status
          }
          eventually {
            withClue(s"Append never completed after having received ${received.size} updates: [${received.mkString(",")}]") {
              completed shouldBe true
            }
          }
          val lastUpdate = received.last

          lastUpdate shouldBe AppendStatus(
            leaderAppendResult = LogAppendSuccess(LogCoords(1, 1), LogCoords(1, 2), List()),
            appended = Map("a" -> AppendEntriesResponse(1, true, 2), "b" -> AppendEntriesResponse(1, true, 2)),
            appendedCoords = Set(LogCoords(term = 1, index = 1), LogCoords(term = 1, index = 2)),
            clusterSize = 2,
            Set(LogCoords(1, 1), LogCoords(1, 2))
          )

          eventually {
            val secondaryValues: Array[String] = follower.handler.log.entriesFrom(1).map(_.data)
            secondaryValues should contain inOrderOnly ("Hello", "World")
          }

          eventually {
            leader.handler.log.latestCommit() shouldBe 2
          }

          // and, just for fun, ensure we replicate to the follower by poking our manual clock
          follower.handler.log.latestCommit() shouldBe 0
          leader.input.onNext(SendHeartbeatTimeout)
          eventually {
            follower.handler.log.latestCommit() shouldBe 2
          }

        } finally {
          cluster.values.foreach(_.close())
        }

      }
    }
  }
}

object RaftPipeMonixTest extends LowPriorityRiffMonixImplicits {

  type PauseHandler[A] = Handlers.PausableHandler[A, RaftNode[A], Handlers.RecordingHandler[A]]
  type PausablePipe[A] = RaftPipe[A, Observer, Observable, Observable, PauseHandler[A]]

  /**
    * Create a cluster of the input nodes, so that each knows about its peers, and sends messages to/receives messages from each of them
    *
    * @param nodes
    * @param execCtxt
    * @tparam A
    * @return
    */
  def asPausableCluster[A: ClassTag](nodes: RaftNode[A]*)(implicit sched: Scheduler): Map[NodeId, PausablePipe[A]] = {

    val ids                     = nodes.map(_.nodeId).toSet
    def duplicates: Set[NodeId] = nodes.groupBy(_.nodeId).filter(_._2.size > 1).keySet
    require(ids.size == nodes.length, s"multiple nodes given w/ the same id: '${duplicates.mkString("[", ",", "]")}'")

    val raftInstById = nodes.map { n =>
      val raftPipe = {

        val timer = new ObservableTimerCallback

        val log: ObservableLog[A] = ObservableLog(n.log)

        val node: RaftNode[A] = {
          n.withTimerCallback(timer) //
            .withCluster(RaftCluster(ids - n.nodeId)) //
            .withLog(log) //
            .withRoleCallback(new ObservableState)
        }

        val raftPipe = {
          type Pausable = Handlers.PausableHandler[A, RaftNode[A], Handlers.RecordingHandler[A]]

          val handler: Pausable = Handlers.pausable(node)

          val appendHandler   = onAppendData(node, log) _
          val pipe            = pipeForHandler[A, Pausable](handler, log, appendHandler)
          val appendResponses = log.appendResults().replay
          appendResponses.connect()
          new RaftPipe[A, Observer, Observable, Observable, PauseHandler[A]](handler, pipe, MonixClient(pipe.input, appendResponses))
        }

        timer.subscribe(raftPipe.nodeId, raftPipe.input)
        raftPipe
      }
      n.nodeId -> raftPipe
    }.toMap

    RaftPipeMonix.wireTogetherMonix(raftInstById)

    raftInstById
  }
}
