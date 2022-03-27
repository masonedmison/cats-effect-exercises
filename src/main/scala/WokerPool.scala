import scala.util.Random
import scala.concurrent.duration._
import cats._
import cats.implicits._
import cats.effect.IO
import cats.effect.Temporal
import cats.effect.kernel.Ref
import cats.effect.std.Queue
import cats.effect.kernel.Deferred
import cats.effect.IOApp
import java.util.UUID
import cats.effect.ExitCode

/** Objective
  *   - Do parallel processing, distributed over a limited number of workers,
  *     each with its own state (counters, DB connections, etc.).
  *
  * Requirements
  *   - Processing jobs must run in parallel
  *   - Submitting a processing request must wait if all workers are busy.
  *   - Submission should do load balancing: wait for the first worker to
  *     finish, not for a certain one.
  *   - Worker should become available whenever a job is completed successfully,
  *     with an exception or cancelled.
  */

trait WorkerPool[A, B] {
  def exec(a: A): IO[B]
  def addWorker(worker: Worker.Worker[A, B]): IO[Worker.WorkerId]
  def removeWorker(workerId: Worker.WorkerId): IO[Unit]
  def removeAllWorkers: IO[Unit]
}

object WorkerPool {

  type WorkerState = Map[Worker.WorkerId, Deferred[IO, Unit]]

  object WorkerState {
    def empty = Map.empty[Worker.WorkerId, Deferred[IO, Unit]]
  }

  private def loopUntilDef[A](io: IO[A], d: Deferred[IO, Unit]): IO[Unit] =
    io.foreverM
      .race(d.get *> IO.println("Deferred completed - halting infinite loop."))
      .void

  // Implement this constructor, and, correspondingly, the interface above.
  // You are free to use named or anonymous classes
  def of[A, B](fs: List[Worker.Worker[A, B]]): IO[WorkerPool[A, B]] =
    (
      Queue
        .bounded[IO, (A, Deferred[IO, Either[Throwable, B]])](fs.length)
        .product(Ref.of[IO, WorkerState](WorkerState.empty))
      )
      .flatMap { case (queue, ref) =>
        {
          fs.parTraverse { worker =>
            val single = queue.take.flatMap { case (inp, resultD) =>
              worker(inp).attempt.flatMap(resultD.complete)
            }
            GenUUID[IO].uuid.product(Deferred[IO, Unit]).flatMap {
              case (uid, workerD) =>
                IO.println(s"Seeding ref with Worker with Id of: $uid") *>
                  ref.update(_.updated(Worker.WorkerId(uid), workerD)) *>
                  loopUntilDef(
                    single,
                    workerD
                  ).start /* for each worker, forever poll queue and process */
            }

          }.void
        }.as {
          new WorkerPool[A, B] {
            def exec(a: A): IO[B] =
              for {
                d <- Deferred[IO, Either[Throwable, B]]
                _ <- queue.offer((a, d))
                res <- d.get.rethrow
              } yield res
            def addWorker(worker: Worker.Worker[A, B]): IO[Worker.WorkerId] =
              for {
                uid <- GenUUID[IO].uuid
                workerId = Worker.WorkerId(uid)
                workerD <- Deferred[IO, Unit]
                _ <- IO.println(s"Adding worker with id: $workerId")
                _ <- ref.update(_.updated(workerId, workerD))
                io <- loopUntilDef(
                  queue.take
                    .flatMap { case (inp, d) =>
                      worker(inp).attempt.flatMap(d.complete)
                    },
                  workerD
                ).start
              } yield workerId
            def removeWorker(workerId: Worker.WorkerId): IO[Unit] =
              for {
                ws <- ref.get.flatMap { ws =>
                  ws.get(workerId) match {
                    case Some(d) => d.complete(())
                    case _ =>
                      new Exception(s"Worker with $workerId does not exist.")
                        .raiseError[IO, Unit]
                  }
                }
                _ <- ref.update(_.removed(workerId))
              } yield ()
            def removeAllWorkers: IO[Unit] =
              ref
                .modify { ws =>
                  WorkerState.empty -> ws.values.toList
                }
                .flatMap { _.parTraverse(_.complete(())) }
                .void

          }
        }
      }

  // Sample test pool to play with in IOApp
  val testPool: IO[WorkerPool[Int, Int]] =
    List
      .range(0, 10)
      .traverse(Worker.mkWorker) /* IO[List[Worker] */
      .flatMap(WorkerPool.of) /* IO[WorkerPool[Int, Int]] */
}

object Worker {

  // To start, our requests can be modelled as simple functions.
  // You might want to replace this type with a class if you go for bonuses. Or not.
  type Worker[A, B] = A => IO[B]
  case class WorkerId(value: UUID) extends AnyVal

  // Sample stateful worker that keeps count of requests it has accepted
  def mkWorker(id: Int)(implicit T: Temporal[IO]): IO[Worker[Int, Int]] =
    Ref[IO].of(0).map { counter =>
      def simulateWork: IO[Unit] =
        IO(50 + Random.nextInt(450)).map(_.millis).flatMap(IO.sleep)

      def report: IO[Unit] =
        counter.get.flatMap(i =>
          IO(
            println(
              s"[${Thread.currentThread()}]Total processed by $id: $i"
            )
          )
        )

      x =>
        IO.println(s"$id is sleeping...") >> simulateWork >>
          counter.update(_ + 1) >>
          report >>
          IO.pure(x + 1)
    }
}

object demo extends IOApp.Simple {
  lazy val tpio = WorkerPool.testPool

  /** this should never terminate
    */
  def removeAll: IO[ExitCode] = {
    for {
      pool <- WorkerPool.of[Unit, Unit](List(_ => IO.sleep(2.seconds)))
      _ <- pool.exec(()).start
      _ <- pool.removeAllWorkers
      _ <- pool.exec(())
    } yield ExitCode.Error
  }

  def addAndRemove: IO[Unit] = {
    val work = (0 to 500).toList
    tpio.flatMap { tp =>
      val newWorker = Worker.mkWorker(999).flatMap(tp.addWorker)

      (IO.sleep(500.millis) *> newWorker.flatMap { wid =>
        IO.sleep(500.millis) *> tp.removeWorker(wid)
      }).start *> work.parTraverse(tp.exec)
    }.void
  }
  def run: IO[Unit] = removeAll.void

}
