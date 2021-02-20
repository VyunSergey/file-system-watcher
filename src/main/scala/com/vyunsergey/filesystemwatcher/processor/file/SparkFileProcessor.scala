package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import cats.syntax.traverse._
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
      _ <- info"Finding Output Meta Directory from Path: '${metaFilePath.toAbsolutePath.toString}'"
      outputMetaPath <- fileProcessor.findParent(metaFilePath, _.getFileName.toString, Some("meta".r)).map(_.getOrElse(metaFilePath.getParent))
      _ <- info"Finding Output Data Directory from Path: '${dataFilePath.toAbsolutePath.toString}'"
      outputDataPath <- fileProcessor.findParent(dataFilePath, _.getFileName.toString, Some("data".r)).map(_.getOrElse(dataFilePath.getParent))
      tempPath = outputMetaPath.getParent.resolve(s"temp")
      _ <- info"Copping Meta DataFiles from Path: ${outputMetaPath.toAbsolutePath.toString} to Path: '${tempPath.toAbsolutePath.toString}'"
      _ <- fileProcessor.copyFiles(outputMetaPath, tempPath)
      _ <- info"Copping Data DataFiles from Path: ${outputDataPath.toAbsolutePath.toString} to Path: '${tempPath.toAbsolutePath.toString}'"
      _ <- fileProcessor.copyFiles(outputDataPath, tempPath, clearTarget = false)
      subPaths <- fileProcessor.getSubPaths(tempPath)
      notDataFiles <- subPaths.traverse { path =>
        (path.pure[F],
          path.getFileName.toString.matches(config.csvMask).pure[F],
          fileProcessor.isExist(path),
          fileProcessor.isFile(path),
          fileProcessor.pathSize(path).map(_ > 0)
        ).tupled
      }.map(_.filter(a => !(a._2 && a._3 && a._4 && a._5)).map(_._1))
      _ <- info"Deleting Not DataFiles: ${notDataFiles.map(_.getFileName.toString).mkString("[", ",", "]")} from Path: '${tempPath.toAbsolutePath.toString}'"
      _ <- notDataFiles.traverse(fileProcessor.deleteFiles)
      _ <- info"Renaming DataFiles with transfer mask in Path: '${tempPath.toAbsolutePath.toString}'"
      dataFiles = subPaths.filter(path => !notDataFiles.contains(path))
      sortedDataFiles <- dataFiles.traverse { path =>
        (path.pure[F],
          fileProcessor.pathLastModifiedTime(path)
        ).tupled
      }.map(_.sortBy(_._2.toMillis).map(_._1))
      _ <- sortedDataFiles.zipWithIndex.traverse { case (path, ind) =>
        fileProcessor.renameFile(path, config.transferFileMask.replaceFirst("<NUM>", ind.toString))
      }
      _ <- info"Finding Output Directory from Path: '${tempPath.toAbsolutePath.toString}'"
      outputPath <- fileProcessor.findParent(tempPath, _.getFileName.toString, Some("out".r)).map(_.getOrElse(tempPath.getParent))
      transferPath = outputPath.getParent.resolve(s"transfer")
      _ <- info"Zipping in archive DataFiles in Path: ${tempPath.toAbsolutePath.toString} to Path: '${transferPath.toAbsolutePath.toString}'"
      resSubPaths <- fileProcessor.getSubPaths(tempPath)
      resDataFiles <- resSubPaths.traverse { path =>
        (path.pure[F],
          fileProcessor.isFile(path)
        ).tupled
      }.map(_.filter(_._2).map(_._1))
      _ <- fileProcessor.zipFiles(resDataFiles, transferPath.resolve(config.transferArchive))
      _ <- info"Creating Marker Transfer File in Path: '${transferPath.toAbsolutePath.toString}'"
      _ <- fileProcessor.deleteFile(transferPath.resolve(config.transferMarker))
      _ <- fileProcessor.createFile(transferPath.resolve(config.transferMarker))
    } yield ()
  }
}

object SparkFileProcessor {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, SparkFileProcessor[F]] =
    Resource.liftF(logs.forService[SparkFileProcessor[F]].map(implicit l => new SparkFileProcessor[F](context)))
}
