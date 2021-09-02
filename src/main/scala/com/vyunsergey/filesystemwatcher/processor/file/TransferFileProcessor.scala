package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.{Resource, Timer}
import cats.syntax.traverse._
import io.circe.syntax._
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.status.ProcessingStatus
import com.vyunsergey.filesystemwatcher.common.transform.TransformerConfig
import tofu.logging.Logging
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.concurrent.duration._

class TransferFileProcessor[F[_]: Monad: Timer: Logging: FileProcessor](context: Context) {
  def processTransferFile(path: Path, transformerConfig: TransformerConfig): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile(_, transformerConfig))

  def processFile(path: Path, transformerConfig: TransformerConfig): F[Unit] = {
    for {
      (config, fileProcessor) <- info"Processing .ZIP Transfer Data Files in Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, implicitly[FileProcessor[F]])
      }
      _ <- info"Finding Product Root Directory from Path: '${path.toAbsolutePath.toString}'"
      productRootPath <- fileProcessor.findParent(path, _.getFileName.toString, Some("product.*".r)).map(_.getOrElse(path.getParent))
      inputPath = productRootPath.resolve("in")
      tempPath = productRootPath.resolve("temp")
      outputPath = productRootPath.resolve("out")
      tempOutPath = outputPath.resolve("temp")
      transferPath = productRootPath.resolve("transfer")
      sdexPath = config.sdexPath
      statusPath = productRootPath.resolve("status")
      backupPath = productRootPath.resolve("backup")
      backupPathIn = backupPath.resolve("in")
      backupPathOut = backupPath.resolve("out")
      currentTime <- fileProcessor.currentTime()
      timeStr <- fileProcessor.formattedTime(currentTime, "yyyy_MM_dd_HH_mm_ss_SSS")
      _ <- info"Backup Input data files in Temp Path: '${tempPath.toAbsolutePath.toString}'"
      tempSubPaths <- fileProcessor.getSubPaths(tempPath)
      tempDataFiles = tempSubPaths.filter(_.getFileName.toString.matches(config.csvMask))
      _ <- if (tempDataFiles.nonEmpty) {
        for {
          _ <- info"Clearing Old Backups in In Path: '${backupPathIn.toAbsolutePath.toString}'"
          _ <- fileProcessor.deleteFiles(backupPathIn)
          _ <- info"Creating New Backup in In Path: '${backupPathIn.toAbsolutePath.toString}'"
          tempDataFileName = tempDataFiles.head.getFileName.toString
          _ <- fileProcessor.zipFiles(tempDataFiles, backupPathIn.resolve(s"$timeStr/$tempDataFileName.zip"))
        } yield ()
      } else ().pure[F]
      _ <- info"Backup Output data files in Transfer Path: '${transferPath.toAbsolutePath.toString}'"
      transferFiles <- List(config.transferArchive, config.transferHash, config.transferMarker)
        .map(transferPath.resolve)
        .traverse(file => fileProcessor.isExist(file)
        .map(flag => (file, flag)))
        .map(_.filter(_._2).map(_._1))
      transferDataFiles = transferFiles.filter(_.getFileName.toString == config.transferArchive)
      _ <- if (transferDataFiles.nonEmpty) {
        for {
          _ <- info"Clearing Old Backups in Out Path: '${backupPathOut.toAbsolutePath.toString}'"
          _ <- fileProcessor.deleteFiles(backupPathOut)
          _ <- info"Creating New Backup in Out Path: '${backupPathOut.toAbsolutePath.toString}'"
          _ <- transferDataFiles.traverse(file => fileProcessor.copyFile(file, backupPathOut.resolve(s"$timeStr/${file.getFileName.toString}")))
        } yield ()
      } else ().pure[F]
      _ <- info"Clear Input Directory in Path: '${inputPath.toAbsolutePath.toString}'"
      inSubPaths <- fileProcessor.getSubPaths(inputPath)
      _ <- inSubPaths.traverse(fileProcessor.deleteFiles)
      _ <- info"Clear Output Directory in Path: '${outputPath.toAbsolutePath.toString}'"
      outSubPaths <- fileProcessor.getSubPaths(outputPath)
      _ <- outSubPaths.traverse(fileProcessor.deleteFiles)
      _ <- info"Deleting Temp Directories: (temp = '${tempPath.toAbsolutePath.toString}', outTemp = '${tempOutPath.toAbsolutePath.toString}')"
      _ <- fileProcessor.deleteFiles(tempPath)
      _ <- fileProcessor.deleteFiles(tempOutPath)
      _ <- info"Copping Transfer Files to SDEX from Path '${transferPath.toAbsolutePath.toString}' to Path '${sdexPath.toAbsolutePath.toString}'"
      _ <- fileProcessor.deleteFiles(sdexPath)
      _ <- transferFiles.traverse(file => fileProcessor.copyFile(file, sdexPath.resolve(file.getFileName)) >> Timer[F].sleep(5.seconds))
      _ <- if (tempDataFiles.nonEmpty && transferDataFiles.nonEmpty && transferFiles.length >= 3) {
        for {
          inputFilesList <- tempDataFiles.map(_.getFileName.toString).mkString("['", "', '", "']").pure[F]
          outputFilesPath <- transferDataFiles.map(_.toAbsolutePath.toString).mkString("['", "', '", "']").pure[F]
          _ <- info"Saving processing Status to Path: '${statusPath.toAbsolutePath.toString}'"
          _ <- fileProcessor.deleteFiles(statusPath)
          _ <- fileProcessor.writeJson(ProcessingStatus.Success(message = s"Successfully processed files: $inputFilesList"))(statusPath.resolve("status.json"))
          _ <- info"[SUCCESS] ---------------------------------------------------------"
          _ <- info"[SUCCESS] Finish processing Input Files: $inputFilesList"
          _ <- info"[SUCCESS] Result: $outputFilesPath"
          _ <- info"[SUCCESS] ---------------------------------------------------------"
        } yield ()
      } else {
        for {
          inputFilesList <- tempDataFiles.map(_.getFileName.toString).mkString("['", "', '", "']").pure[F]
          _ <- info"Saving processing Status to Path: '${statusPath.toAbsolutePath.toString}'"
          _ <- fileProcessor.deleteFiles(statusPath)
          _ <- fileProcessor.writeJson(ProcessingStatus.Failure(message = s"Error while processing input files: $inputFilesList. Check that 1) Files exist and 2) All files have the same CSV format: ${transformerConfig.reader.asJson.noSpaces} and 3) Total size of all files does not exceed 512Mb."))(statusPath.resolve("status.json"))
          _ <- error"Can`t processing input files: $inputFilesList form Paths: (in = '${inputPath.toAbsolutePath.toString}', temp = '${tempPath.toAbsolutePath.toString}', transfer = '${transferPath.toAbsolutePath.toString}')"
        } yield ()
      }
    } yield ()
  }
}

object TransferFileProcessor {
  def apply[F[_]: Monad: Timer: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, TransferFileProcessor[F]] =
    Resource.eval(logs.forService[TransferFileProcessor[F]].map(implicit l => new TransferFileProcessor[F](context)))
}
