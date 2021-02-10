package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.Transformer
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class CsvFileProcessor[F[_]: Monad: Logging: FileProcessor: Transformer](context: Context) {
  def processCsvFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] = {
    for {
      (config, fileProcessor) <- info"Finding productId for Path: '${path.toAbsolutePath.toString}'" as {
        (context.config, implicitly[FileProcessor[F]])
      }
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
                for {
                  transformerCommand <- transformer.createCommand(
                    config.transformer.mode,
                    config.transformer.jarPath,
                    transformerConfig,
                    config.transformer.command
                  )
                  _ <- info"Finding Input Directory from Path: '${path.toAbsolutePath.toString}'"
                  inputPath <- fileProcessor.findParent(path, _.getFileName.toString, Some("in".r))
                  targetPath = inputPath.getOrElse(path).getParent.resolve("out")
                  _ <- info"Executing Transformer Command: $transformerCommand"
                  _ <- transformer.exec(transformerCommand, path, targetPath)
                } yield ()
            }
          } yield ()
      }
    } yield ()
  }
}

object CsvFileProcessor {
  def apply[F[_]: Monad: FileProcessor: Transformer](context: Context, logs: Logs[F, F]): Resource[F, CsvFileProcessor[F]] =
    Resource.liftF(logs.forService[CsvFileProcessor[F]].map(implicit l => new CsvFileProcessor[F](context: Context)))
}
