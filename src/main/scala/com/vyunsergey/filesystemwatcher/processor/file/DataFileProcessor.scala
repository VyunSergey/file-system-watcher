package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.Transformer
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.util.{Failure, Success, Try}

class DataFileProcessor[F[_]: Monad: Logging: FileProcessor: Transformer: CsvFileProcessor: ZipFileProcessor](context: Context) {
  def processDataFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] =
    for {
      (config, configPath, fileProcessor) <- info"Processing Data File in Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, context.configPath, implicitly[FileProcessor[F]])
      }
      csvFileMasks <- Try(config.fileMask("csv")) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Can`t get 'csv' files mask from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getClass.getName}: ${exp.getMessage}"
          } yield throw exp
      }
      zipFileMasks <- Try(config.fileMask("zip")) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Can`t get 'zip' files mask from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getClass.getName}: ${exp.getMessage}"
          } yield throw exp
      }
      fileName = path.getFileName.toString
      (csvDataFileMasks, zipDataFileMasks) = (csvFileMasks.dataFile.r.regex, zipFileMasks.dataFile.r.regex)
      (isCsvDataFile, isZipDataFile) = (fileName.matches(csvDataFileMasks), fileName.matches(zipDataFileMasks))
      _ <- if (isCsvDataFile || isZipDataFile) {
        for {
          _ <- info"Finding productId for Path: '${path.toAbsolutePath.toString}'"
          configPathOp <- fileProcessor.findParent(path, _.toAbsolutePath.toString.replace("\\", "/"), Some(config.productMask.r))
          _ <- configPathOp match {
            case None => error"Can`t find productId for Path: '${path.toAbsolutePath.toString}' with Mask: '${config.productMask}'"
            case Some(configPath) =>
              for {
                transformer <- info"Creating Transformer" as implicitly[Transformer[F]]
                productId = configPath.getFileName.toString
                transformerConfigOp <- transformer.createConfig(productId)
                _ <- transformerConfigOp match {
                  case None => error"Can`t create Transformer Config from Path: '${path.toAbsolutePath.toString}'"
                  case Some(transformerConfig) =>
                    if (isCsvDataFile) {
                      for {
                        csvFileProcessor <- info"Going process .CSV Data File in Path: '${path.toAbsolutePath.toString}'" as {
                          implicitly[CsvFileProcessor[F]]
                        }
                        _ <- csvFileProcessor.processCsvFile(path, transformerConfig)
                      } yield ()
                    } else if (isZipDataFile) {
                      for {
                        zipFileProcessor <- info"Going process .ZIP Data File in Path: '${path.toAbsolutePath.toString}'" as {
                          implicitly[ZipFileProcessor[F]]
                        }
                        _ <- zipFileProcessor.processZipFile(path, transformerConfig)
                      } yield ()
                    } else ().pure[F]
                }
              } yield ()
          }
        } yield ()
      } else info"Path: '${path.toAbsolutePath.toString}' with File: $fileName does not matches data file masks: ($csvDataFileMasks, $zipDataFileMasks)"
    } yield ()
}

object DataFileProcessor {
  def apply[F[_]: Monad: FileProcessor: Transformer: CsvFileProcessor: ZipFileProcessor](context: Context, logs: Logs[F, F]): Resource[F, DataFileProcessor[F]] =
    Resource.liftF(logs.forService[DataFileProcessor[F]].map(implicit l => new DataFileProcessor[F](context)))
}
