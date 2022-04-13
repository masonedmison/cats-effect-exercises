import cats.effect.IO
import cats.effect.Concurrent
import cats.effect.kernel.Deferred
import cats.implicits._
import cats.effect.kernel.Ref
import State.Done
import cats.effect.{Temporal, Spawn}
import cats.effect.IOApp
import scala.concurrent.duration._
import cats.effect.syntax._
import cats.effect.instances._
import cats.effect.kernel.MonadCancel

trait CountdownLatch[F[_]] {
  def await: F[Unit]
  def decrement: F[Unit]
}

sealed trait State[F[_]]
object State {
  final case class Outstanding[F[_]](n: Long, whenDone: Deferred[F, Unit])
      extends State[F]
  case class Done[F[_]]() extends State[F]
}

object CountdownLatch {
  def instance[F[_]: Concurrent: Temporal](
      n: Long
  ): F[CountdownLatch[F]] =
    for {
      d <- Deferred[F, Unit]
      ref <- Ref.of[F, State[F]](State.Outstanding(n, d))
    } yield new CountdownLatch[F] {
      def await: F[Unit] =
        ref.get
          .flatMap {
            case State.Done()                   => ().pure[F]
            case State.Outstanding(_, whenDone) => whenDone.get
          }
      // add sleep to exacerbate problem of deferred never getting completed
      def decrement: F[Unit] = MonadCancel[F].uncancelable { poll =>
        ref.modify {
          case State.Outstanding(1, whenDone) =>
            Done[F]() -> (Temporal[F].sleep(200.millis) *> whenDone.complete(
              ()
            )).void
          case State.Outstanding(n, whenDone) =>
            State.Outstanding(n - 1, whenDone) -> ().pure[F]
          case State.Done() =>
            State.Done() -> ().pure[F]
        }.flatten
      }
    }
}

object LatchDemo extends IOApp.Simple {
  def prerequisites(latch: CountdownLatch[IO]): IO[Unit] =
    for {
      _ <- IO.println("Awaiting prerequisites")
      _ <- latch.await
      _ <- IO.println("Finished with prerequisites")
    } yield ()

  def execPrerequisiteAndCancel(latch: CountdownLatch[IO]): IO[Unit] =
    for {
      _ <- IO.println("Decrementing latch for prereqs")
      fib <- latch.decrement.start
      _ <- fib.cancel
      _ <- IO.println("Oh noes I have been cancelled!")
      _ <- IO.println("Finished latch decrement")
    } yield ()

  def execPrerequisite(latch: CountdownLatch[IO]): IO[Unit] =
    for {
      _ <- IO.println("Decrementing latch for prereqs")
      fib <- latch.decrement
      _ <- IO.println("Finished latch decrement")
    } yield ()

  def run: IO[Unit] = {
    val latch = CountdownLatch.instance[IO](1)
    latch.flatMap { latch =>
      (prerequisites(latch), execPrerequisiteAndCancel(latch)).parTupled.void
    }
  }

}
