package riff.raft.integration.simulator

import org.reactivestreams.{Publisher, Subscriber}
import riff.raft.{AppendStatus, RaftClient}

import scala.concurrent.duration.FiniteDuration

class SimulatorClient(simulator: RaftSimulator, defaultLatency: FiniteDuration) extends RaftClient[String] {
  override def append(data: String): Publisher[AppendStatus] = {
    simulator.appendToLeader(Array(data), defaultLatency) match {
      case None =>
        ??? // error
        new Publisher[AppendStatus] {
          override def subscribe(s: Subscriber[_ >: AppendStatus]): Unit = {}
        }
      case Some(coords) =>
        // tail
        new Publisher[AppendStatus] {
          override def subscribe(s: Subscriber[_ >: AppendStatus]): Unit = {}
        }
    }
  }
}
