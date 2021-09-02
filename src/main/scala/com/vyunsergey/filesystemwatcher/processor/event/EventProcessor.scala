package com.vyunsergey.filesystemwatcher.processor.event

import cats.Monad
import cats.effect.Resource
import cats.syntax.semigroup._
import com.vyunsergey.filesystemwatcher.processor.file.MarkerFileProcessor
import fs2.io.Watcher.Event
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.{Path, WatchEvent}

class EventProcessor[F[_]: Monad: Logging: MarkerFileProcessor] {
  import EventProcessor._

  def process(event: Event): F[Unit] =
    for {
      e <- info"Received Event $event" as event
      _ <- e match {
        case Event.Created(path: Path, _: Int) =>
          for {
            _ <- debug"Got Created path '${path.toAbsolutePath.toString}'"
            markerFileProcessor <- info"Going process Marker File in Path: '${path.toAbsolutePath.toString}'" as {
              implicitly[MarkerFileProcessor[F]]
            }
            _ <- markerFileProcessor.processMarkerFile(path)
          } yield ()
        case _ => info"Waiting Events: [Created], Skipping Event: $event"
      }
    } yield ()
}

object EventProcessor {
  def apply[F[_]: Monad: MarkerFileProcessor](logs: Logs[F, F]): Resource[F, EventProcessor[F]] =
    Resource.eval(logs.forService[EventProcessor[F]].map(implicit l => new EventProcessor[F]))

  implicit val argumentsLoggable: DictLoggable[Event] = new DictLoggable[Event] {
    override def fields[I, V, R, S](a: Event, i: I)(implicit r: LogRenderer[I, V, R, S]): R = (a: @unchecked) match {
      case Event.Created(path: Path, count: Int) =>
        r.addString("event", "Created", i) |+|
          r.addString("path", path.toAbsolutePath.toString, i) |+|
          r.addString("count", count.toString, i)
      case Event.Deleted(path: Path, count: Int) =>
        r.addString("event", "Deleted", i) |+|
          r.addString("path", path.toAbsolutePath.toString, i) |+|
          r.addString("count", count.toString, i)
      case Event.Modified(path: Path, count: Int) =>
        r.addString("event", "Modified", i) |+|
          r.addString("path", path.toAbsolutePath.toString, i) |+|
          r.addString("count", count.toString, i)
      case Event.Overflow(count: Int) =>
        r.addString("event", "Overflow", i) |+|
          r.addString("path", "", i) |+|
          r.addString("count", count.toString, i)
      case Event.NonStandard(event: WatchEvent[_], registeredDirectory: Path) =>
        r.addString("event", event.kind.name, i) |+|
          r.addString("path", registeredDirectory.toAbsolutePath.toString, i) |+|
          r.addString("count", "0", i)
    }

    override def logShow(a: Event): String = (a: @unchecked) match {
      case Event.Created(path: Path, count: Int) => s"Created(path = '${path.toAbsolutePath.toString}', count = '$count')"
      case Event.Deleted(path: Path, count: Int) => s"Deleted(path = '${path.toAbsolutePath.toString}', count = '$count')"
      case Event.Modified(path: Path, count: Int) => s"Modified(path = '${path.toAbsolutePath.toString}', count = '$count')"
      case Event.Overflow(count: Int) => s"Overflow(count = '$count')"
      case Event.NonStandard(event: WatchEvent[_], registeredDirectory: Path) =>
        s"NonStandard(event = '${event.kind.name}', registeredDirectory = '${registeredDirectory.toAbsolutePath.toString}')"
    }
  }
}
