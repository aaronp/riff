package riff.raft.log
import java.nio.file.Path

/**
  * Represents the return type of a file-based raft log
  *
  * @param written
  * @param replaced
  */
final case class LogAppendResult(written: Path, replaced: Seq[Path] = Nil)
