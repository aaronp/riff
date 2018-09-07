package riff.raft.node

class PersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState[String] => Unit) : Unit = {
    withClue("in memory") {
      test(PersistentState.inMemory[String]())

    }

    withClue("cached") {
      test(PersistentState.inMemory[String]().cached())
    }
  }
}

