package riff

import java.nio.file.Path
import java.util.UUID

import eie.io.LowPriorityIOImplicits

class RiffSpec extends BaseSpec with LowPriorityIOImplicits {

  def nextTestDir(name: String) = {
    s"target/test/${name}-${UUID.randomUUID()}".asPath
  }

  def withDir[T](f: Path => T): T = {

    val name: String = getClass.getSimpleName
    val dir: Path = nextTestDir(name)
    if (dir.exists()) {
      dir.delete()
    }
    dir.mkDirs()
    try {
      f(dir)
    } finally {
      dir.delete()
    }
  }
}
