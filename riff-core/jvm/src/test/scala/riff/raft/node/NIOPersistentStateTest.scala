package riff.raft.node

class NIOPersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState[String] => Unit) = {
    withDir { dir =>
      val st8 = NIOPersistentState[String](dir, true)
      test(st8)
    }
  }
}
