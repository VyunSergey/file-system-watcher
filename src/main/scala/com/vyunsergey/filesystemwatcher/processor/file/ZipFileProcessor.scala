package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import cats.syntax.traverse._
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.{Transformer, TransformerConfig}
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class ZipFileProcessor[F[_]: Monad: Logging: FileProcessor: Transformer](context: Context) {
  def processZipFile(path: Path, transformerConfig: TransformerConfig): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile(_, transformerConfig))

  def processFile(path: Path, transformerConfig: TransformerConfig): F[Unit] = {
    for {
      (config, fileProcessor) <- info"Processing .ZIP file in Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, implicitly[FileProcessor[F]])
      }
      _ <- info"Finding Input Directory from Path: '${path.toAbsolutePath.toString}'"
      inputPath <- fileProcessor.findParent(path, _.getFileName.toString, Some("in".r)).map(_.getOrElse(path.getParent))
      fileName <- fileProcessor.clearFileName(path.getFileName.toString)
      tempPath = inputPath.getParent.resolve(s"temp/$fileName")
      _ <- info"Unzipping archive ${path.getFileName.toString} to Path: '${tempPath.toAbsolutePath.toString}'"
      _ <- fileProcessor.deleteFiles(tempPath.getParent)
      _ <- fileProcessor.unzipFiles(path, tempPath)
      subPaths <- fileProcessor.getSubPaths(tempPath)
      notDataFiles = subPaths.filter(!_.getFileName.toString.matches(config.csvMask))
      _ <- info"Deleting Not Data Files: ${notDataFiles.map(_.getFileName.toString).mkString("[", ",", "]")} from Path: '${tempPath.toAbsolutePath.toString}'"
      _ <- notDataFiles.traverse(fileProcessor.deleteFiles)
      _ <- info"Creating Source Directory from Path: '${path.toAbsolutePath.toString}'"
      sourceName = s"${transformerConfig.productId}__$fileName"
      sourcePath = tempPath.getParent.resolve(sourceName)
      _ <- fileProcessor.deleteFiles(sourcePath)
      _ <- fileProcessor.renameFile(tempPath, sourceName)
      _ <- info"Creating Target Directory from Path: '${inputPath.toAbsolutePath.toString}'"
      targetPath = inputPath.getParent.resolve("out")
      transformer <- info"Creating Transformer" as implicitly[Transformer[F]]
      transformerCommand <- transformer.createCommand(
        config.transformer.mode,
        config.transformer.jarPath,
        transformerConfig,
        config.transformer.command
      )
      _ <- info"Executing Transformer Command: $transformerCommand"
      _ <- transformer.exec(transformerCommand, sourcePath, targetPath)
    } yield ()
  }
}

object ZipFileProcessor {
  def apply[F[_]: Monad: FileProcessor: Transformer](context: Context, logs: Logs[F, F]): Resource[F, ZipFileProcessor[F]] =
    Resource.liftF(logs.forService[ZipFileProcessor[F]].map(implicit l => new ZipFileProcessor[F](context)))
}
