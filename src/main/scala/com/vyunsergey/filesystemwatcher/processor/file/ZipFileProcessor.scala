package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import cats.syntax.traverse._
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.Transformer
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.util.{Failure, Success, Try}

class ZipFileProcessor[F[_]: Monad: Logging: FileProcessor: Transformer](context: Context) {
  def processZipFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] = {
    for {
      (config, configPath, fileProcessor) <- info"Finding productId for Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, context.configPath, implicitly[FileProcessor[F]])
      }
      csvFileMasks <- Try(config.fileMask("csv")) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Can`t get 'csv' files mask from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getClass.getName}: ${exp.getMessage}"
          } yield throw exp
      }
      configPathOp <- fileProcessor.findParent(path, _.toAbsolutePath.toString.replace("\\", "/"), Some(config.productMask.r))
      _ <- configPathOp match {
        case None => error"Can`t find productId for Path: '${path.toAbsolutePath.toString}' with Mask: '${config.productMask}'"
        case Some(configPath) =>
          for {
            _ <- info"Finding Input Directory from Path: '${path.toAbsolutePath.toString}'"
            inputPath <- fileProcessor.findParent(path, _.getFileName.toString, Some("in".r)).map(_.getOrElse(path))
            fileName <- fileProcessor.clearFileName(path.getFileName.toString)
            tempPath = inputPath.getParent.resolve(s"temp/$fileName")
            _ <- info"Unzipping archive ${path.getFileName.toString} to Path: '${tempPath.toAbsolutePath.toString}'"
            _ <- fileProcessor.unzipFiles(path, tempPath)
            subPaths <- fileProcessor.getSubPaths(tempPath)
            notDataFiles = subPaths.filter(!_.getFileName.toString.matches(csvFileMasks.dataFile))
            _ <- info"Deleting Not Data Files: ${notDataFiles.map(_.getFileName.toString).mkString("[", ",", "]")} from Path: '${tempPath.toAbsolutePath.toString}'"
            _ <- notDataFiles.traverse(fileProcessor.deleteFile)
            transformer <- info"Creating Transformer" as implicitly[Transformer[F]]
            productId = configPath.getFileName.toString
            transformerConfigOp <- transformer.createConfig(productId)
            _ <- transformerConfigOp match {
              case None => error"Can`t create Transformer Config from Path: '${path.toAbsolutePath.toString}'"
              case Some(transformerConfig) =>
                for {
                  transformerCommand <- transformer.createCommand(
                    config.transformer.mode,
                    config.transformer.jarPath,
                    transformerConfig,
                    config.transformer.command
                  )
                  _ <- info"Creating Source Directory from Path: '${path.toAbsolutePath.toString}'"
                  sourceName = s"${productId}__$fileName"
                  sourcePath = tempPath.getParent.resolve(sourceName)
                  _ <- fileProcessor.deleteFile(sourcePath)
                  _ <- fileProcessor.renameFile(tempPath, sourceName)
                  _ <- info"Creating Target Directory from Path: '${path.toAbsolutePath.toString}'"
                  targetPath = inputPath.getParent.resolve("out")
                  _ <- info"Executing Transformer Command: $transformerCommand"
                  _ <- transformer.exec(transformerCommand, sourcePath, targetPath)
                } yield ()
            }
          } yield ()
      }
    } yield ()
  }
}

object ZipFileProcessor {
  def apply[F[_]: Monad: FileProcessor: Transformer](context: Context, logs: Logs[F, F]): Resource[F, ZipFileProcessor[F]] =
    Resource.liftF(logs.forService[ZipFileProcessor[F]].map(implicit l => new ZipFileProcessor[F](context: Context)))
}
