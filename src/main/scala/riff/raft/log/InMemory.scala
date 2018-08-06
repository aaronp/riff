package riff.raft.log

private[log] class InMemory[T]() extends BaseLog[T] {
  override type Result = Boolean
  private var entries       = List[(LogCoords, T)]()
  private var lastCommitted = 0
  override protected def doCommit(index: Int): Unit = {
    lastCommitted = index
  }

  override def append(coords: LogCoords, data: T): Boolean = {
    val indicesToDelete = checkForOverwrite(coords)
    entries = indicesToDelete.foldLeft(entries) {
      case (list, i) => list.dropWhile(_._1.index >= i)
    }
    entries match {
      case (head, _) :: _ =>
        require(coords.index == head.index + 1)
        entries = (coords, data) :: entries
      case tail =>
        require(coords.index == 1)
        entries = (coords, data) :: tail
    }
    true
  }
  override def latestCommit(): Int = {
    lastCommitted
  }

  override def termForIndex(index: Int): Option[Int] = {
    entries.collectFirst {
      case (LogCoords(term, `index`), _) => term
    }
  }
  override def latestAppended(): LogCoords = entries.headOption.map(_._1).getOrElse(LogCoords.Empty)
}
