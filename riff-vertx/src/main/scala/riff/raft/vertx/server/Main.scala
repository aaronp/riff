package riff.raft.vertx.server
import com.typesafe.scalalogging.StrictLogging

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {
    RunningVertxService.start(args, 8000) match {
      case None =>
        sys.error(s"Usage: Expected the name and an optional cluster size but got '${args.mkString(" ")}'")
      case Some(running) =>
        logger.info(s"Started ${running}")
    }
  }

}
