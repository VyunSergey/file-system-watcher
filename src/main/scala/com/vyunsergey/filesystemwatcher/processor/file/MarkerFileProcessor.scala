package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.util.{Failure, Success, Try}

class MarkerFileProcessor[F[_]: Monad: Logging: FileProcessor: DataFileProcessor](context: Context) {
  def processMarkerFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] =
    for {
      (config, configPath) <- (context.config, context.configPath).pure[F]
      csvFileMasks <- Try(config.fileMask("csv")) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Cant get 'csv' files mask from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getMessage}"
          } yield throw exp
      }
      zipFileMasks <- Try(config.fileMask("zip")) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Cant get 'zip' files mask from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getMessage}"
          } yield throw exp
      }
      fileName = path.getFileName.toString
      (csvMarkerFileMasks, zipMarkerFileMasks) = (csvFileMasks.markerFile.r.regex, zipFileMasks.markerFile.r.regex)
      (isCsvMarkerFile, isZipMarkerFile) = (fileName.matches(csvMarkerFileMasks), fileName.matches(zipMarkerFileMasks))
      _ <- if (isCsvMarkerFile || isZipMarkerFile) {
        for {
          fileProcessor <- implicitly[FileProcessor[F]].pure[F]
          dataFileMatchRegex <- debug"Creating Data File Match regex" as {
            (fileName.split("\\.").reverse.tail.reverse.mkString("\\.") + ".*").r
          }
          dataFileMismatchRegex <- debug"Creating Data File Mismatch regex" as {
            (".*\\." + fileName.split("\\.").last).r
          }
          _ <- info"Finding Path of Data File in Path: '${path.getParent.toAbsolutePath.toString}'"
          subPathOp <- fileProcessor.find(path.getParent, Some(dataFileMatchRegex), Some(dataFileMismatchRegex))
          _ <- if (subPathOp.nonEmpty) {
            for {
              subPath <- subPathOp.get.pure[F]
              _ <- info"Find Data File with Path: '${subPath.toAbsolutePath.toString}'"
              dataFileProcessor <- info"Going process Data File in Path: '${subPath.toAbsolutePath.toString}'" as {
                implicitly[DataFileProcessor[F]]
              }
              _ <- dataFileProcessor.processDataFile(subPath)
            } yield ()
          } else error"No Data File in Path: '${path.getParent.toAbsolutePath.toString}'!"
        } yield ()
      } else info"Path: '${path.toAbsolutePath.toString}' with File: $fileName does not matches marker file masks: ($csvMarkerFileMasks, $zipMarkerFileMasks)"
    } yield ()
}

object MarkerFileProcessor {
  def apply[F[_]: Monad: FileProcessor: DataFileProcessor](context: Context, logs: Logs[F, F]): Resource[F, MarkerFileProcessor[F]] =
    Resource.liftF(logs.forService[MarkerFileProcessor[F]].map(implicit l => new MarkerFileProcessor[F](context)))
}
