package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class SparkFileProcessor[F[_]: Monad: Logging: FileProcessor](context: Context) {
  def processSparkFile(metaFilePath: Path, dataFilePath: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(metaFilePath, dataFilePath)(processFile)

  def processFile(metaFilePath: Path, dataFilePath: Path): F[Unit] = {
    for {
      (config, fileProcessor) <- info"Processing Spark .CSV files in Paths: (meta = '${metaFilePath.toAbsolutePath.toString}', data = '${dataFilePath.toAbsolutePath.toString}')" as {
        (context.config, implicitly[FileProcessor[F]])
      }
      _ <- println(
        s"""
           |*** IN SparkFileProcessor.processFile ***
           |meta Path: '${metaFilePath.toAbsolutePath.toString}'
           |data Path: '${dataFilePath.toAbsolutePath.toString}'
           |
           |""".stripMargin).pure[F]
    } yield ()
  }
}

object SparkFileProcessor {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, SparkFileProcessor[F]] =
    Resource.liftF(logs.forService[SparkFileProcessor[F]].map(implicit l => new SparkFileProcessor[F](context)))
}
