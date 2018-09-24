package riff.raft.node

class PersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState => Unit) : Unit = {
    withClue("in memory") {
      test(PersistentState.inMemory())
    }

    withClue("cached") {
      test(PersistentState.inMemory().cached())
    }
  }
}

