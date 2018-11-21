package riff.runtime

object RiffApp {

  def main(a: Array[String]): Unit = {
    val port = a.headOption.map(_.toInt).getOrElse(8080)

  }

}
