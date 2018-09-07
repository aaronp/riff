package riff.raft.log
import org.scalajs.dom.raw.Storage

/**
  * Abstraction from Storage for testing -- you probably don't have to do this,
  * but you can't just 'new Storage'
  *
  * @tparam A
  */
trait Repo {
  def setItem(key: String, value: String)
  def getItem(key: String): Option[String]
  def removeItem(key: String): Unit
}
object Repo {
  class Instance[A: StringFormat](storage: Storage) extends Repo {
    override def setItem(key: String, value: String): Unit = {
      storage.setItem(key, value)
    }
    override def getItem(key: String): Option[String] = {
      Option(storage.getItem(key))
    }
    override def removeItem(key: String): Unit = storage.removeItem(key)
  }
  def apply[A: StringFormat](storage: Storage) = new Instance(storage)

  def apply[A: StringFormat](): Repo = new Repo {
    val fmt         = StringFormat[A]
    private var map = Map[String, String]()
    override def setItem(key: String, value: String): Unit = {
      map = map.updated(key, value)
    }
    override def getItem(key: String): Option[String] = {
      map.get(key)
    }
    override def removeItem(key: String): Unit = map = map - key
  }
}
