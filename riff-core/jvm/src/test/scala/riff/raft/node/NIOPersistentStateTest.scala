package riff.raft.node

class NIOPersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState => Unit) = {
    withDir { dir =>
      val file = dir.resolve("y").createIfNotExists().append("hi")
      file.append("there").append("!")
      val x = file.text

      println(file)
      println(x)

      val st8 = NIOPersistentState(dir, true)
      test(st8)
    }
  }
}
