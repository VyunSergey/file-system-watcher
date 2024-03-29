package com.vyunsergey.filesystemwatcher.watcher

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource}
import fs2.Stream
import fs2.io.Watcher.Event
import fs2.io.{Watcher, file}
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.{Path, StandardCopyOption, Files => JFiles}
import scala.util.Try

class FileSystemWatcher[F[_]: ConcurrentEffect: ContextShift: Logging](blocker: Blocker) {
  def watch(path: Path): Resource[F, Stream[F, Watcher.Event]] = {
    for {
      watcher <- file.watcher(blocker).flatMap { watcher =>
        Resource.eval(
          info"Watching Path '${path.toAbsolutePath.toString}'" as {
            watcher
          }
        )
      }
      _ <- Resource.eval(watcher.watch(path))
    } yield watcher.events()
  }

  def watchWithFileSize(path: Path): Resource[F, Stream[F, (Watcher.Event, Long)]] =
    watch(path).map(_.evalMap(withFileSize))

  def withFileSize(event: Watcher.Event): F[(Watcher.Event, Long)] = event match {
    case Event.Created(path: Path, _: Int) => fileSize(path).map((event, _))
    case Event.Deleted(path: Path, _: Int) => fileSize(path).map((event, _))
    case Event.Modified(path: Path, _: Int) => fileSize(path).map((event, _))
    case Event.Overflow(_: Int) => (event, 0L).pure[F]
    case Event.NonStandard(_, registeredDirectory: Path) =>
      fileSize(registeredDirectory).map((event, _))
  }

  def isFile(path: Path): F[Boolean] =
    Try(JFiles.isRegularFile(path)).getOrElse(false).pure[F]

  def fileSize(path: Path): F[Long] =
    for {
      isFile <- isFile(path)
      size <- if (isFile) Try(file.size(blocker, path)).toOption.getOrElse(0L.pure[F]) else 0L.pure[F]
    } yield size

  def rename(file: Path, name: String): F[Unit] =
    for {
      isFile <- isFile(file)
      targetName = file.getParent.resolve(name)
    } yield {
      if (isFile) {
        Try(JFiles.move(file, targetName, StandardCopyOption.REPLACE_EXISTING)).getOrElse(file).pure[F]
      }
    }
}

object FileSystemWatcher {
  def apply[F[_]: ConcurrentEffect: ContextShift](blocker: Blocker, logs: Logs[F, F]): Resource[F, FileSystemWatcher[F]] = {
    Resource.eval(logs.forService[FileSystemWatcher[F]].map(implicit l => new FileSystemWatcher[F](blocker)))
  }
}
