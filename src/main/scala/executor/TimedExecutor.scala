package executor

import java.time.LocalDateTime
import java.util.concurrent._

import executor.impl.TimedExecutorImpl

import scala.collection.parallel.availableProcessors

/** A service that accepts events as pairs of ([[java.time.LocalDateTime]], [[java.util.concurrent.Callable]])
  * and executes each event's {@code Callable} no earlier than the specified {@code LocalDateTime}. The tasks should be
  * be executed according to the value of the {@code LocalDateTime} or in the order of events' arrival if their
  * {@code LocalDateTime}s are equal. Events may arrive in arbitrary order and the [[executor.TimedExecutor#enqueue]]
  * method may be called from different threads.
  * Wittingly overdue events are not accepted and no events are being timed out.
  */
trait TimedExecutor {
  /**
    * Stops the executor. The executor won't accept tasks after it's been stopped.
    */
  def stop(): Unit
  def isStopped: Boolean

  /**
    * Enqueues events to be executed.
    *
    * @param event the event to be executed
    * @throws java.lang.IllegalArgumentException if the event is null or its instant or task are null
    * @throws java.lang.IllegalArgumentException if the event is wittingly overdue
    * @throws java.lang.IllegalStateException if the executor has been stopped
    * @return a [[java.util.concurrent.CompletableFuture]] representing the result of the event's task execution
    */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[IllegalStateException])
  def enqueue[T](event: (LocalDateTime, Callable[T])): CompletableFuture[T]
}

object TimedExecutor {
  def apply() = new TimedExecutorImpl(availableProcessors)
}