import java.util.UUID
import cats.implicits._
import cats.effect.Sync

trait GenUUID[F[_]] {
  def uuid: F[UUID]
}

object GenUUID {
  def apply[F[_]](implicit ev: GenUUID[F]) = ev
  implicit def forWorkerPool[F[_]: Sync]: GenUUID[F] = new GenUUID[F] {
    def uuid: F[UUID] = Sync[F].delay(UUID.randomUUID())
  }

}
