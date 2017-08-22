package executor.impl

import java.time.{Clock, Instant, LocalDateTime, ZoneId}
import java.util.concurrent.{ArrayBlockingQueue, Callable, CompletableFuture, ExecutorService}

import org.mockito.Matchers.isA
import org.mockito.Mockito.{verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.Conductors
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, OneInstancePerTest}

class TimedExecutorImplSpec extends FlatSpec with Matchers with MockitoSugar with Conductors
  with BeforeAndAfterEach with OneInstancePerTest {
  "Timed executor" should "not enqueue invalid events" in {
    val past = LocalDateTime.ofInstant(zeroInstant minusMillis 1L, zone)
    the[IllegalArgumentException] thrownBy executor.enqueue(null) should have message "requirement failed: Null event"
    the[IllegalArgumentException] thrownBy executor.enqueue((null, null)) should have message "requirement failed: Null instant"
    the[IllegalArgumentException] thrownBy executor.enqueue((past, null)) should have message "requirement failed: Null task"
    the[IllegalArgumentException] thrownBy executor.enqueue((past, task(()))) should have message "requirement failed: Overdue event"
  }

  it should "not enqueue events after it's been stopped" in {
    executor.stop()
    the[IllegalStateException] thrownBy executor.enqueue(
      (LocalDateTime.now() plusYears 1L, task(()))
    ) should have message "Executor has been stopped"
  }

  it should "shutdown the pool on stop" in {
    executor.stop()
    executor.isStopped shouldBe true
    executorThread.join()
    verify(poolMock).shutdown()
  }

  it should """execute tasks according to the value of their execution times or in the order of their arrival
              |  if the times are equal (events may arrive in arbitrary order and the enqueue method may be called from
              |  different threads)""".stripMargin in {
    val conductedPartSize = 1024
    val freeRunningPartSize = 2048
    val results = new ArrayBlockingQueue[Int](conductedPartSize + freeRunningPartSize)

    val conductor = new Conductor
    import conductor._

    val startInstant = Instant.now
    val startTime    = LocalDateTime.ofInstant(startInstant, zone)

    threadNamed("odd producer") {
      val conductedPart = for(tick <- 1 until conductedPartSize by 2) yield
        executor.enqueue((startTime plusNanos tick, task(results.offer(tick))))
      waitForBeat(2)
      val freeRunningPart = for(tick <- conductedPartSize + 1 until conductedPartSize + freeRunningPartSize by 2) yield
        executor.enqueue((startTime plusNanos tick, task(results.offer(tick))))
      waitForBeat(3)
      CompletableFuture.allOf(conductedPart ++ freeRunningPart: _*).join()
    }

    threadNamed("even producer") {
      waitForBeat(1)
      val conductedPart = for(tick <- 0 until conductedPartSize by 2) yield
        executor.enqueue((startTime plusNanos tick, task(results.offer(tick))))
      waitForBeat(2)
      val freeRunningPart = for(tick <- conductedPartSize until conductedPartSize + freeRunningPartSize by 2) yield
        executor.enqueue((startTime plusNanos tick, task(results.offer(tick))))
      waitForBeat(3)
      CompletableFuture.allOf(conductedPart ++ freeRunningPart: _*).join()
    }

    threadNamed("clock") {
      waitForBeat(2)
      executor.clock = Clock.fixed(startInstant plusNanos conductedPartSize, zone)
      waitForBeat(3)
      executor.clock = Clock.fixed(startInstant plusNanos conductedPartSize + freeRunningPartSize, zone)
    }

    whenFinished{
      results.toArray shouldEqual Array.range(0, conductedPartSize + freeRunningPartSize)
    }
  }

  private val zeroInstant = Instant.now
  private val zone = ZoneId.systemDefault()

  @volatile private var executorThread: Thread = _
  private val poolMock = when(mock[ExecutorService].submit(isA(classOf[Runnable]))).thenAnswer((iom: InvocationOnMock) =>
    iom.getArgumentAt(0, classOf[Runnable]) match {
      case tei: TimedExecutorImpl =>
        executorThread = new Thread(tei)
        executorThread.start()
        new CompletableFuture[Unit]()
      case ec@EventCallback(_, promise) =>
        ec.run()
        promise
  }).getMock[ExecutorService]
  private val executor = new TimedExecutorImpl(Clock.fixed(zeroInstant, zone), poolMock)
  verify(poolMock).submit(executor)

  def task[T](outcome: => T): Callable[T] = () => outcome

  override protected def afterEach(): Unit = executor.stop()
}
