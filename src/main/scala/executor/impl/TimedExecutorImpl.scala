package executor.impl

import java.time.temporal.ChronoUnit.NANOS
import java.time.{Clock, LocalDateTime}
import java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import executor.TimedExecutor

import scala.util.Try

private[impl] case class QueuedEvent(id: Long, runAt: LocalDateTime, callback: EventCallback[_]) extends Comparable[QueuedEvent]{
  assert(runAt != null && callback != null)

  override def compareTo(anotherEvent: QueuedEvent): Int = {
    val cmp = runAt.compareTo(anotherEvent.runAt)
    if(cmp == 0) id.compareTo(anotherEvent.id) else cmp
  }
}

private[impl] case class EventCallback[T](task: Callable[T], promise: CompletableFuture[T]) extends Runnable {
  override def run(): Unit =
    try {
      val outcome = task.call()
      promise.complete(outcome)
    } catch {
      case t: Throwable => promise.completeExceptionally(t)
    }

  def cancel(): Unit = promise.cancel(false)
}

private[executor] class TimedExecutorImpl private[impl](@volatile private[impl] var clock: Clock, pool: ExecutorService)
                                                        extends TimedExecutor with Runnable {
  private[executor] def this(parallelism: Int) =
    this(Clock.systemDefaultZone(), new ForkJoinPool(parallelism, defaultForkJoinWorkerThreadFactory, null, true))

  private val eventId    = new AtomicLong
  private val queueLock  = new ReentrantLock
  private val schedule   = queueLock.newCondition()
  private val eventQueue = new PriorityBlockingQueue[QueuedEvent]

  @volatile private var isRunning = true
  pool.submit(this)

  /**
    * Stops the executor. The executor won't accept tasks after it's been stopped.
    */
  override def stop(): Unit = {
    clearRunning()
    awakenIf(true)
  }
  override def isStopped: Boolean = !isRunning

  private def clearRunning(): Unit = isRunning = false
  private def cancelAllTasks(): Unit = eventQueue.toArray(Array.empty[QueuedEvent]).foreach(_.callback.cancel())

  /**
    * Enqueues events to be executed.
    *
    * @param event the event to be executed
    * @throws java.lang.IllegalArgumentException if the event is null or its instant or task are null
    * @throws java.lang.IllegalArgumentException if the event is wittingly overdue
    * @throws java.lang.IllegalStateException    if the executor has been stopped
    * @return a [[java.util.concurrent.CompletableFuture]] representing the result of the event's task execution
    */
  override def enqueue[T](event: (LocalDateTime, Callable[T])): CompletableFuture[T] = {
    require(event    != null, "Null event")
    require(event._1 != null, "Null instant")
    require(event._2 != null, "Null task")
    require(!event._1.isBefore(now), "Overdue event")

    if(isStopped) throw new IllegalStateException("Executor has been stopped")

    val promise = new CompletableFuture[T]()
    val scheduledEvent = QueuedEvent(eventId.getAndIncrement(), event._1, EventCallback[T](event._2, promise))
    awakenIf {
      val previousHead = Option(eventQueue.peek())
      eventQueue.offer(scheduledEvent)
      previousHead.forall(_.runAt isAfter scheduledEvent.runAt)
    }
    promise
  }

  override def run(): Unit = {
    try {
      while(isRunning) {
        val lock = queueLock
        lock.lockInterruptibly()
        var headEvent: Option[QueuedEvent] = Option(eventQueue.peek())
        try while(isRunning && headEvent.forall(_.runAt isAfter now)) {
          headEvent.fold { schedule.await() } { e => schedule.awaitNanos(until(e.runAt)) }
          headEvent = Option(eventQueue.peek())
        } finally {
          lock.unlock()
        }

        if(isRunning) {
          val event = eventQueue.take
          pool.submit(event.callback)
        }
      }
    } finally {
      clearRunning()
      cancelAllTasks()
      pool.shutdown()
    }
  }

  private def now = LocalDateTime.now(clock)

  private def awakenIf(condition: => Boolean): Unit = {
    val lock = queueLock
    lock.lock()
    try if(condition) schedule.signal()
    finally lock.unlock()
  }

  private def until(ldt: LocalDateTime) = Try(now.until(ldt, NANOS)).getOrElse(Long.MaxValue)
}