package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class MarkerFileProcessor[F[_]: Monad: Logging: FileProcessor: DataFileProcessor](context: Context) {
  def processMarkerFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] =
    for {
      (config, fileProcessor) <- info"Processing Marker File in Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, implicitly[FileProcessor[F]])
      }
      csvFileMasks <- config.csvFileMasks[F]
      zipFileMasks <- config.zipFileMasks[F]
      sparkMetaFileMasks <- config.sparkMetaFileMasks[F]
      sparkDataFileMasks <- config.sparkDataFileMasks[F]
      transferFileMasks <- config.transferFileMasks[F]
      fileName = path.getFileName.toString
      filePath = path.toAbsolutePath.toString.replace("\\", "/")
      (csvMarkerFileMasks, zipMarkerFileMasks, transferMarkerFileMasks) = (csvFileMasks.markerFile.r.regex, zipFileMasks.markerFile.r.regex, transferFileMasks.markerFile.r.regex)
      (sparkMetaMarkerFileMasks, sparkDataMarkerFileMasks) = (sparkMetaFileMasks.markerFile.r.regex, sparkDataFileMasks.markerFile.r.regex)
      (sparkMetaDataFileMasks, sparkDataDataFileMasks) = (sparkMetaFileMasks.dataFile.r.regex, sparkDataFileMasks.dataFile.r.regex)
      (isCsvMarkerFile, isZipMarkerFile, isTransferMarkerFile) = (filePath.matches(csvMarkerFileMasks), filePath.matches(zipMarkerFileMasks), filePath.matches(transferMarkerFileMasks))
      (isSparkMetaMarkerFile, isSparkDataMarkerFile) = (filePath.matches(sparkMetaMarkerFileMasks), filePath.matches(sparkDataMarkerFileMasks))
      _ <- if (isCsvMarkerFile || isZipMarkerFile || isTransferMarkerFile) {
        for {
          _ <- debug"Creating Data File Match regex"
          dataFileMatchRegex <- fileProcessor.clearFileName(fileName).map(nm => (nm + ".*").r)
          dataFileMismatchRegex <- debug"Creating Data File Mismatch regex" as {
            (".*\\.(done|md5|" + fileName.split("\\.").last + ")").r
          }
          _ <- info"Finding Path of Data File in Path: '${path.getParent.toAbsolutePath.toString}'"
          subPathOp <- fileProcessor.find(
            path.getParent,
            _.getFileName.toString,
            _.isRegularFile,
            Some(dataFileMatchRegex),
            Some(dataFileMismatchRegex)
          )
          _ <- if (subPathOp.nonEmpty) {
            for {
              dataFileProcessor <- info"Going process DataFile in Path: '${subPathOp.get.toAbsolutePath.toString}'" as {
                implicitly[DataFileProcessor[F]]
              }
              _ <- dataFileProcessor.processDataFile(subPathOp.get)
            } yield ()
          } else error"No Data File in Path: '${path.getParent.toAbsolutePath.toString}'!"
        } yield ()
      } else if (isSparkMetaMarkerFile || isSparkDataMarkerFile) {
        if (isSparkMetaMarkerFile) {
          for {
            sparkDataMarkerFileMatchRegex <- debug"Creating Spark Data MarkerFile Match regex" as sparkDataMarkerFileMasks.r
            _ <- info"Finding Path of Spark Data MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
            markerSubPathOp <- fileProcessor.find(
              path.getParent.getParent,
              _.toAbsolutePath.toString.replace("\\", "/"),
              _.isRegularFile,
              Some(sparkDataMarkerFileMatchRegex),
              None
            )
            _ <- if (markerSubPathOp.nonEmpty) {
              for {
                sparkDataDataFileMatchRegex <- debug"Creating Spark Data DataFile Match regex" as sparkDataDataFileMasks.r
                _ <- info"Finding Path of Spark Data DataFile in Path: '${markerSubPathOp.get.getParent.toAbsolutePath.toString}'"
                dataSubPathOp <- fileProcessor.find(
                  markerSubPathOp.get.getParent,
                  _.toAbsolutePath.toString.replace("\\", "/"),
                  _.isRegularFile,
                  Some(sparkDataDataFileMatchRegex),
                  None
                )
                _ <- if (dataSubPathOp.nonEmpty) {
                  for {
                    dataFileProcessor <- info"Going process Spark Files in Path: '${dataSubPathOp.get.toAbsolutePath.toString}'" as {
                      implicitly[DataFileProcessor[F]]
                    }
                    _ <- dataFileProcessor.processDataFile(dataSubPathOp.get)
                  } yield ()
                } else error"No Spark Data DataFile in Path: '${markerSubPathOp.get.getParent.toAbsolutePath.toString}'!"
              } yield ()
            } else info"No Spark Data MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
          } yield ()
        } else if (isSparkDataMarkerFile) {
          for {
            sparkMetaMarkerFileMatchRegex <- debug"Creating Spark Meta MarkerFile Match regex" as sparkMetaMarkerFileMasks.r
            _ <- info"Finding Path of Spark Meta MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
            markerSubPathOp <- fileProcessor.find(
              path.getParent.getParent,
              _.toAbsolutePath.toString.replace("\\", "/"),
              _.isRegularFile,
              Some(sparkMetaMarkerFileMatchRegex),
              None
            )
            _ <- if (markerSubPathOp.nonEmpty) {
              for {
                sparkMetaDataFileMatchRegex <- debug"Creating Spark Meta DataFile Match regex" as sparkMetaDataFileMasks.r
                _ <- info"Finding Path of Spark Meta DataFile in Path: '${markerSubPathOp.get.getParent.toAbsolutePath.toString}'"
                dataSubPathOp <- fileProcessor.find(
                  markerSubPathOp.get.getParent,
                  _.toAbsolutePath.toString.replace("\\", "/"),
                  _.isRegularFile,
                  Some(sparkMetaDataFileMatchRegex),
                  None
                )
                _ <- if (dataSubPathOp.nonEmpty) {
                  for {
                    dataFileProcessor <- info"Going process Spark Files in Path: '${dataSubPathOp.get.toAbsolutePath.toString}'" as {
                      implicitly[DataFileProcessor[F]]
                    }
                    _ <- dataFileProcessor.processDataFile(dataSubPathOp.get)
                  } yield ()
                } else error"No Spark Meta DataFile in Path: '${markerSubPathOp.get.getParent.toAbsolutePath.toString}'!"
              } yield ()
            } else info"No Spark Meta MarkerFile in Path: '${path.getParent.getParent.toAbsolutePath.toString}'"
          } yield ()
        } else ().pure[F]
      } else info"Path: '$filePath' does not matches MarkerFile masks: ('$csvMarkerFileMasks', '$zipMarkerFileMasks', '$sparkMetaMarkerFileMasks', '$sparkDataMarkerFileMasks', '$transferMarkerFileMasks')"
    } yield ()
}

object MarkerFileProcessor {
  def apply[F[_]: Monad: FileProcessor: DataFileProcessor](context: Context, logs: Logs[F, F]): Resource[F, MarkerFileProcessor[F]] =
    Resource.eval(logs.forService[MarkerFileProcessor[F]].map(implicit l => new MarkerFileProcessor[F](context)))
}
