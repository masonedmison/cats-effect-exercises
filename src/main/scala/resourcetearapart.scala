import cats.effect.kernel.Resource
import scala.concurrent.duration._

import cats.effect.{IO, ExitCode, IOApp, Concurrent}

import cats.effect.Sync
import cats.effect.Temporal
import cats.effect.implicits._
import cats.implicits._
import cats.Monad
import cats.effect.kernel.Ref
import cats.effect.kernel.Outcome
import cats.effect.kernel.Resource.ExitCase
import cats.effect.kernel.Deferred
import cats.effect.kernel.Async
import cats.Applicative
import cats.effect.std.Queue
import cats.effect.std.Supervisor
import cats.effect.std.Console
import cats.effect.kernel.MonadCancel

sealed trait Scope[F[_]] {
  def open[A](ra: Resource[F, A]): F[A]
}

object Scope {
  // Implement this. Add context bounds as necessary.
  def apply[F[_]: Async: Console]: Resource[F, Scope[F]] =
    Resource
      .make {
        Ref.of[F, List[F[Unit]]] {
          List.empty[F[Unit]]
        }
      } { ref =>
        ref.get
          .flatMap {
            _.foldRight(false.pure[F]) { case (fin, acc) =>
              fin.attempt.flatMap {
                case Right(_) => acc.map(_ || false)
                case _        => acc.map(_ || true)
              }
            }
          }
          .flatMap { b =>
            if (b) new Exception("exc in finalizer").raiseError[F, Unit]
            else ().pure[F]
          }
      }
      .flatMap { ref =>
        Resource.eval {
          new Scope[F] {
            def open[A](ra: Resource[F, A]): F[A] =
              MonadCancel[F].uncancelable { poll =>
                ra.allocated.flatMap { case (a, fin) =>
                  ref.update(fin :: _) *>
                    Console[F].println(s"using $a") *> poll(Sync[F].delay(a))
                }
              }
          }.pure[F]
        }
      }
}

object Main extends IOApp {
  case class TestResource(idx: Int)

  override def run(args: List[String]): IO[ExitCode] = {
    happyPath[IO] >>
      atomicity[IO] >>
      cancelability[IO] >>
      errorInRelease[IO] >>
      errorInAcquire[IO] >>
      IO(println("Run completed")) >>
      IO.pure(ExitCode.Success)
  }

  case class Allocs[F[_]](
      normal: Resource[F, TestResource],
      slowAcquisition: Resource[F, TestResource],
      crashOpen: Resource[F, TestResource],
      crashClose: Resource[F, TestResource]
  )

  def happyPath[F[_]: Async: Temporal: Console] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.slowAcquisition)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        println(allocs)
        println(deallocs)
        require(allocs == Vector(1, 2, 3))
        require(allocs == deallocs.reverse)
        require(ec == ExitCase.Succeeded)
      }
    }

  def atomicity[F[_]: Async: Temporal: Console] =
    test[F] { (allocs, scope, cancelMe) =>
      for {
        r1 <- scope.open(allocs.normal)
        lock <- Deferred[F, Unit]
        _ <- (lock.get >> cancelMe).start
        r2 <- scope.open(
          Resource.liftK(lock.complete(())) >> allocs.slowAcquisition
        )
        _ <- Temporal[F].sleep(1.second)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        println(allocs)
        println(deallocs)
        require(allocs == Vector(1, 2))
        require(deallocs == Vector(2, 1))
        require(ec == ExitCase.Canceled)
      }
    }

  def cancelability[F[_]: Async: Temporal: Console] =
    test[F] { (allocs, scope, cancelMe) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.slowAcquisition)
        _ <- cancelMe
        _ <- Temporal[F].sleep(1.second)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        require(allocs == Vector(1, 2))
        require(deallocs == Vector(2, 1))
        require(ec == ExitCase.Canceled)
      }
    }

  def errorInRelease[F[_]: Async: Temporal: Console] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.crashClose)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        println(ec)
        println(allocs)
        println(deallocs)
        require(allocs == Vector(1, 2, 3))
        require(deallocs == Vector(3, 1))
        require(ec match {
          case ExitCase.Errored(_) => true
          case _                   => false
        })
      }
    }

  def errorInAcquire[F[_]: Async: Temporal: Console] =
    test[F] { (allocs, scope, _) =>
      for {
        r1 <- scope.open(allocs.normal)
        r2 <- scope.open(allocs.crashOpen)
        r3 <- scope.open(allocs.normal)
      } yield ()
    } { (allocs, deallocs, ec) =>
      Sync[F].delay {
        println(ec)
        println(allocs)
        println(deallocs)
        require(allocs == Vector(1))
        require(deallocs == Vector(1))
        require(ec match {
          case ExitCase.Errored(_) => true
          case _                   => false
        })
      }
    }

  def test[F[_]: Async: Temporal: Console](
      run: (Allocs[F], Scope[F], F[Unit]) => F[Unit]
  )(
      check: (Vector[Int], Vector[Int], ExitCase) => F[Unit]
  ): F[Unit] =
    for {
      idx <- Ref.of[F, Int](1)
      allocLog <- Ref.of[F, Vector[Int]](Vector.empty[Int])
      deallocLog <- Ref.of[F, Vector[Int]](Vector.empty[Int])
      open = idx
        .modify(i => (i + 1, i))
        .flatTap(i => allocLog.update(_ :+ i))
        .map(TestResource)
      close = (r: TestResource) => deallocLog.update(_ :+ r.idx)
      cancel <- Deferred[F, Unit]
      slow = Temporal[F].sleep(1.second)
      allocs = Allocs[F](
        Resource.make(open)(close),
        Resource.make(slow >> open)(close),
        Resource.eval(Concurrent[F].raiseError[TestResource](new Exception)),
        Resource.make(open)(_ => Concurrent[F].raiseError(new Exception))
      )
      finish <- Deferred[F, Either[Throwable, Unit]]
      scope = for {
        _ <- Resource.makeCase(().pure[F])((_, ec) => {
          (allocLog.get, deallocLog.get)
            .mapN(check(_, _, ec))
            .flatten
            .attempt
            .flatMap(finish.complete(_).void)
        })
        s <- Scope[F]
      } yield s
      _ <- scope
        .use(run(allocs, _, cancel.complete(()).void))
        .race(cancel.get)
        .attempt
      _ <- finish.get.rethrow
    } yield ()
}
