package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import cats.syntax.traverse._
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging.Logging
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class TransferFileProcessor[F[_]: Monad: Logging: FileProcessor](context: Context) {
  def processTransferFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] = {
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
      transferSubPaths <- fileProcessor.getSubPaths(transferPath)
      transferDataFiles = transferSubPaths.filter(_.getFileName.toString.matches(config.zipMask))
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
      _ <- if (tempDataFiles.nonEmpty && transferDataFiles.nonEmpty) {
        for {
          _ <- info"[SUCCESS] ---------------------------------------------------------"
          _ <- info"[SUCCESS] Finish processing Input Files: '${tempDataFiles.map(_.getFileName.toString).mkString("['", "', '", "']")}'"
          _ <- info"[SUCCESS] Result: '${transferDataFiles.map(_.toAbsolutePath.toString).mkString("['", "', '", "']")}'"
          _ <- info"[SUCCESS] ---------------------------------------------------------"
        } yield ()
      } else ().pure[F]
    } yield ()
  }
}

object TransferFileProcessor {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, TransferFileProcessor[F]] =
    Resource.liftF(logs.forService[TransferFileProcessor[F]].map(implicit l => new TransferFileProcessor[F](context)))
}
