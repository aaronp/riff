package riff.raft.node

class NIOPersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState => Unit) = {
    withDir { dir =>
      val st8 = NIOPersistentState(dir, true)
      test(st8)
    }
  }
}
