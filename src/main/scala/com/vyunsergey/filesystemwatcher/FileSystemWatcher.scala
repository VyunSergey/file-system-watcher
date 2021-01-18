package com.vyunsergey.filesystemwatcher

import cats.effect.Async
import cats.implicits._
import fs2.Stream
import fs2.io.Watcher
import fs2.io.Watcher.Event._
import fs2.io.file.Files

import java.nio.file.{Files => JFiles, Path, StandardCopyOption}
import scala.util.Try

trait FileSystemWatcher[F[_]] {
  def watch(path: Path)(implicit AF: Async[F]): F[Stream[F, Watcher.Event]] =
    Files[F].watch(path).pure[F]

  def watchWithFileSize(path: Path)(implicit AF: Async[F]): F[Stream[F, (Watcher.Event, Long)]] =
    watch(path).map(_.evalMap(withFileSize))

  def withFileSize(event: Watcher.Event)(implicit AF: Async[F]): F[(Watcher.Event, Long)] = event match {
    case Created(path: Path, _: Int) => fileSize(path).map((event, _))
    case Deleted(path: Path, _: Int) => fileSize(path).map((event, _))
    case Modified(path: Path, _: Int) => fileSize(path).map((event, _))
    case Overflow(_: Int) => (event, 0L).pure[F]
    case NonStandard(_, registeredDirectory: Path) =>
      Files[F].size(registeredDirectory).map((event, _))
  }

  def isFile(path: Path)(implicit AF: Async[F]): F[Boolean] =
    AF.delay(java.nio.file.Files.isRegularFile(path))

  def fileSize(path: Path)(implicit AF: Async[F]): F[Long] =
    for {
      isFile <- isFile(path)
      size <- if (isFile) Try(Files[F].size(path)).toOption.getOrElse(0L.pure[F]) else 0L.pure[F]
    } yield size

  def rename(file: Path, name: String)(implicit AF: Async[F]): F[Unit] =
  for {
    isFile <- isFile(file)
  } yield if (isFile) AF.pure(JFiles.move(file, file.getParent.resolve(name), StandardCopyOption.REPLACE_EXISTING))
}

object FileSystemWatcher {
  def apply[F[_]](implicit AF: Async[F]): FileSystemWatcher[F] = new FileSystemWatcher[F] {}
}
