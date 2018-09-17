package riff.raft.node

class NIOPersistentStateTest extends PersistentStateTCK {

  def withPersistentState(test: PersistentState[String] => Unit) = {
    withDir { dir =>

    val file = dir.resolve("y").createIfNotExists().append("hi")
      file.append("there").append("!")
      val x = file.text

      println(file)
      println(x)

      val st8 = NIOPersistentState[String](dir, true)
      test(st8)
    }
  }
}
