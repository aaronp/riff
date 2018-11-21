package riff.raft.node
import com.typesafe.scalalogging.StrictLogging
import riff.raft.NodeId
import riff.raft.messages.RaftMessage

import scala.util.control.NonFatal

case class RecordingMessageHandler[A](underlying: RaftMessageHandler[A]) extends RaftMessageHandler[A] with StrictLogging with AutoCloseable {
  private var requests: List[RaftMessage[A]] = Nil

  private var responses: List[RaftNodeResult[A]] = Nil

  private def getStack(): String = {
    val t = Thread.currentThread()

    t.getStackTrace.tail.tail.mkString(s"${t.getName} call stack:\n\t\t* ", "\n\t\t* ", "\n")
  }

  @volatile private var handling  = false
  @volatile private var callStack = getStack()

  override def nodeId: NodeId = underlying.nodeId

  override def onMessage(input: RaftMessage[A]): Result = {
    require(!handling, "not single-threaded! : " + callStack)

    callStack = getStack()

    handling = true
    requests = input :: requests
    val response = try {
      underlying.onMessage(input)
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error on message $input: $e")

        (requests.tail zip responses).foreach {
          case tuple @ (in, out) =>
            logger.error(s"""${nodeId} >>
                            |$in
                            |--> $out
                            |""".stripMargin)
        }

        throw e
    }
    responses = response :: responses

    handling = false
    response
  }

  override def close(): Unit = {
    underlying match {
      case closable: AutoCloseable => closable.close()
      case _                       =>
    }
  }
}
