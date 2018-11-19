package riff.raft.node
import java.util.concurrent.atomic.AtomicBoolean

import riff.raft.NodeId
import riff.raft.messages.RaftMessage

object Handlers {

  def pausable[A, H <: RaftMessageHandler[A]](underlying: H): PausableHandler[A, H, RecordingHandler[A]] = {
    val paused = FixedHandler[A](underlying.nodeId, NoOpResult(s"${underlying.nodeId} is paused"))
    new PausableHandler(underlying, new RecordingHandler(paused))
  }

  case class FixedHandler[A](override val nodeId: NodeId, fixedResult: RaftNodeResult[A]) extends RaftMessageHandler[A] {
    override def onMessage(ignore: RaftMessage[A]): Result = fixedResult
  }

  case class PausableHandler[A, H1 <: RaftMessageHandler[A], H2 <: RaftMessageHandler[A]](underlying: H1, pausedHandler: H2) extends RaftMessageHandler[A] with AutoCloseable {
    override def nodeId: NodeId = underlying.nodeId

    private val paused = new AtomicBoolean(false)

    def pause() = {
      paused.compareAndSet(false, true)
    }

    def resume() = {
      paused.compareAndSet(true, false)
    }
    override def onMessage(input: RaftMessage[A]): Result = {
      val result = if (paused.get) {
        pausedHandler.onMessage(input)
      } else {
        underlying.onMessage(input)
      }
      println(s"${underlying.nodeId} (paused=$paused) on $input ---> $result")

      result
    }

    override def close(): Unit = {
      underlying match {
        case closable : AutoCloseable => closable.close()
        case _ =>
      }
      pausedHandler match {
        case closable : AutoCloseable => closable.close()
        case _ =>
      }
    }
  }

  /**
    *
    * @param underlying
    * @tparam A the type of data which is appended to the log (could just be a byte array, some union type, etc)
    */
  class RecordingHandler[A](underlying: RaftMessageHandler[A]) extends RaftMessageHandler[A] with AutoCloseable {
    private var requestsList: List[RaftMessage[A]] = Nil
    private var responsesList: List[RaftNodeResult[A]] = Nil

    override def nodeId: NodeId = underlying.nodeId

    def requests() = requestsList
    def responses() = responsesList

    override def onMessage(input: RaftMessage[A]): Result = {
      requestsList = input :: requestsList

      val response = underlying.onMessage(input)
      responsesList = response :: responsesList
      response
    }
    override def close(): Unit = {
      underlying match {
        case closable : AutoCloseable => closable.close()
        case _ =>
      }
    }
  }

}
