package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.util.{Failure, Success, Try}

class DataFileProcessor[F[_]: Monad: Logging: FileProcessor: CsvFileProcessor: ZipFileProcessor](context: Context) {
  def processDataFile(path: Path): F[Unit] =
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
      (csvDataFileMasks, zipDataFileMasks) = (csvFileMasks.dataFile.r.regex, zipFileMasks.dataFile.r.regex)
      (isCsvDataFile, isZipDataFile) = (fileName.matches(csvDataFileMasks), fileName.matches(zipDataFileMasks))
      _ <- if (isCsvDataFile || isZipDataFile) {
        if (isCsvDataFile) {
          for {
            csvFileProcessor <- info"Going process .CSV Data File in Path: '${path.toAbsolutePath.toString}'" as {
              implicitly[CsvFileProcessor[F]]
            }
            _ <- csvFileProcessor.processCsvFile(path)
          } yield ()
        } else if (isZipDataFile) {
          for {
            zipFileProcessor <- info"Going process .ZIP Data File in Path: '${path.toAbsolutePath.toString}'" as {
              implicitly[ZipFileProcessor[F]]
            }
            _ <- zipFileProcessor.processZipFile(path)
          } yield ()
        } else ().pure[F]
      } else info"Path: '${path.toAbsolutePath.toString}' with File: $fileName does not matches data file masks: ($csvDataFileMasks, $zipDataFileMasks)"
    } yield ()
}

object DataFileProcessor {
  def apply[F[_]: Monad: FileProcessor: CsvFileProcessor: ZipFileProcessor](context: Context, logs: Logs[F, F]): Resource[F, DataFileProcessor[F]] =
    Resource.liftF(logs.forService[DataFileProcessor[F]].map(implicit l => new DataFileProcessor[F](context: Context)))
}
