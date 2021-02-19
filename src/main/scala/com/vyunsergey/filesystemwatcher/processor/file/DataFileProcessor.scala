package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.Transformer
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class DataFileProcessor[F[_]: Monad: Logging:
                        FileProcessor:
                        Transformer:
                        CsvFileProcessor:
                        ZipFileProcessor:
                        SparkFileProcessor:
                        TransferFileProcessor](context: Context) {
  def processDataFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] =
    for {
      (config, fileProcessor) <- info"Processing Data File in Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, implicitly[FileProcessor[F]])
      }
      csvFileMasks <- config.csvFileMasks[F]
      zipFileMasks <- config.zipFileMasks[F]
      sparkMetaFileMasks <- config.sparkMetaFileMasks[F]
      sparkDataFileMasks <- config.sparkDataFileMasks[F]
      transferFileMasks <- config.transferFileMasks[F]
      filePath = path.toAbsolutePath.toString.replace("\\", "/")
      (csvDataFileMasks, zipDataFileMasks, transferDataFileMasks) = (csvFileMasks.dataFile.r.regex, zipFileMasks.dataFile.r.regex, transferFileMasks.dataFile.r.regex)
      (sparkMetaDataFileMasks, sparkDataDataFileMasks) = (sparkMetaFileMasks.dataFile.r.regex, sparkDataFileMasks.dataFile.r.regex)
      (isCsvDataFile, isZipDataFile, isTransferDataFile) = (filePath.matches(csvDataFileMasks), filePath.matches(zipDataFileMasks), filePath.matches(transferDataFileMasks))
      (isSparkMetaDataFile, isSparkDataDataFile) = (filePath.matches(sparkMetaDataFileMasks), filePath.matches(sparkDataDataFileMasks))
      _ <- if (isCsvDataFile || isZipDataFile || isSparkMetaDataFile || isSparkDataDataFile || isTransferDataFile) {
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
                    } else if (isSparkMetaDataFile) {
                      for {
                        sparkDataDataFileMatchRegex <- debug"Creating Spark Data DataFile Match regex" as sparkDataDataFileMasks.r
                        _ <- info"Finding Path of Spark Meta MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
                        dataSubPathOp <- fileProcessor.find(
                          path.getParent.getParent,
                          _.toAbsolutePath.toString.replace("\\", "/"),
                          _.isRegularFile,
                          Some(sparkDataDataFileMatchRegex),
                          None
                        )
                        _ <- if (dataSubPathOp.nonEmpty) {
                          for {
                            sparkFileProcessor <- info"Going process .CSV Spark Data Files in Paths: (meta = '${path.toAbsolutePath.toString}', data = '${dataSubPathOp.get.toAbsolutePath.toString}')" as {
                              implicitly[SparkFileProcessor[F]]
                            }
                            _ <- sparkFileProcessor.processSparkFile(path, dataSubPathOp.get)
                          } yield ()
                        } else error"No Spark Data DataFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'!"
                      } yield ()
                    } else if (isSparkDataDataFile) {
                      for {
                        sparkMetaDataFileMatchRegex <- debug"Creating Spark Data DataFile Match regex" as sparkMetaDataFileMasks.r
                        _ <- info"Finding Path of Spark Meta MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
                        dataSubPathOp <- fileProcessor.find(
                          path.getParent.getParent,
                          _.toAbsolutePath.toString.replace("\\", "/"),
                          _.isRegularFile,
                          Some(sparkMetaDataFileMatchRegex),
                          None
                        )
                        _ <- if (dataSubPathOp.nonEmpty) {
                          for {
                            sparkFileProcessor <- info"Going process .CSV Spark Data Files in Paths: (meta = '${dataSubPathOp.get.toAbsolutePath.toString}', data = '${path.toAbsolutePath.toString}')" as {
                              implicitly[SparkFileProcessor[F]]
                            }
                            _ <- sparkFileProcessor.processSparkFile(dataSubPathOp.get, path)
                          } yield ()
                        } else error"No Spark Meta DataFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'!"
                      } yield ()
                    } else if (isTransferDataFile) {
                      for {
                        transferFileProcessor <- info"Going process .ZIP Transfer Data Files in Path: '${path.toAbsolutePath.toString}'" as {
                          implicitly[TransferFileProcessor[F]]
                        }
                        _ <- transferFileProcessor.processTransferFile(path, transformerConfig)
                      } yield ()
                    } else ().pure[F]
                }
              } yield ()
          }
        } yield ()
      } else info"Path: '$filePath' does not matches DataFile masks: ('$csvDataFileMasks', '$zipDataFileMasks', '$sparkMetaDataFileMasks', '$sparkDataDataFileMasks', '$transferDataFileMasks')"
    } yield ()
}

object DataFileProcessor {
  def apply[F[_]: Monad:
            FileProcessor:
            Transformer:
            CsvFileProcessor:
            ZipFileProcessor:
            SparkFileProcessor:
            TransferFileProcessor](context: Context, logs: Logs[F, F]): Resource[F, DataFileProcessor[F]] =
    Resource.liftF(logs.forService[DataFileProcessor[F]].map(implicit l => new DataFileProcessor[F](context)))
}
