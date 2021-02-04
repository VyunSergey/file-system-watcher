package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class CsvFileProcessor[F[_]: Monad: Logging: FileProcessor](context: Context) {
  def processCsvFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] = {
    for {
      _ <- context.config.pure[F]
      _ <- info"*** PROCESSING .CSV FILE ${path.getFileName.toString} ***"
    } yield ()
  }
}

object CsvFileProcessor {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, CsvFileProcessor[F]] =
    Resource.liftF(logs.forService[CsvFileProcessor[F]].map(implicit l => new CsvFileProcessor[F](context: Context)))
}
