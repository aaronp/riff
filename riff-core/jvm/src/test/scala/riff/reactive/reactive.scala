package riff
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import org.reactivestreams.tck.TestEnvironment

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

package object reactive {

  val DEFAULT_TIMEOUT_MILLIS = 50L
  val DEFAULT_NO_SIGNALS_TIMEOUT_MILLIS: Long = DEFAULT_TIMEOUT_MILLIS
  val PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 250L

  val testEnv = new TestEnvironment(DEFAULT_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS)

  val ids = new AtomicInteger(0)

  private def setName(prefix: String)(t: Thread) = {
    t.setName(s"$prefix${ids.incrementAndGet()}")
    t
  }

  def newContextWithThreadPrefix(threadPrefix: String): ExecutionContextExecutorService = {
    newContext(setName(threadPrefix))
  }

  def newContext(init: Thread => Thread = setName(getClass.getSimpleName)): ExecutionContextExecutorService = {
    val factory = new ThreadFactory {

      override def newThread(r: Runnable): Thread = {
        init(new Thread(r))
      }
    }
    val es: ExecutorService = Executors.newCachedThreadPool(factory)
    ExecutionContext.fromExecutorService(es)
  }
}
