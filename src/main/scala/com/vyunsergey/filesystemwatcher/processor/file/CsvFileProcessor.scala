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

class CsvFileProcessor[F[_]: Monad: Logging: FileProcessor: Transformer](context: Context) {
  def processCsvFile(path: Path): F[Unit] =
    implicitly[FileProcessor[F]].process(path)(processFile)

  def processFile(path: Path): F[Unit] = {
    for {
      (config, configPath) <- (context.config, context.configPath).pure[F]
      productRegexps <- info"Getting .csv options for Products" as {
        config.transformer.options.keys.toList.map { key =>
          (key, config.productMask.replaceFirst("<PRODUCT_ID>", key).r)
        }
      }
      fileProcessor <- info"Finding .csv options for Path: '${path.toAbsolutePath.toString}'" as {
        implicitly[FileProcessor[F]]
      }
      keyPathOp <- productRegexps.traverse { case (key, regex) =>
        fileProcessor.find(
          path,
          _.toAbsolutePath.toString.replace("\\", "/"),
          _.isRegularFile,
          Some(regex)).map((key, _)
        )
      }.map(_.find(_._2.isDefined))
      _ <- keyPathOp match {
        case None => error"Cant find .csv options for Path: '${path.toAbsolutePath.toString}' from Config: $config form Path: '${configPath.toAbsolutePath.toString}'"
        case Some((key, _)) =>
          for {
            _ <- Try(config.transformer.options(key)) match {
              case Failure(exp) => error"Cant get '$key' transformer options from Config: $config form Path: '${configPath.toAbsolutePath.toString}'. ${exp.getMessage}"
              case Success(options) =>
                for {
                  transformer <- info"Creating Transformer" as implicitly[Transformer[F]]
                  transformerCommand <- transformer.createCommand(
                    config.transformer.mode,
                    config.transformer.jarPath,
                    options.mkString(" "),
                    config.transformer.command
                  )
                  _ <- info"Executing Transformer Command: $transformerCommand"
                  targetPath <- fileProcessor.findParent(path, _.getFileName.toString, Some("in".r))
                  _ <- info"Find Parent Path: '${targetPath.map(_.toAbsolutePath.toString)}'"
                  _ <- transformer.exec(transformerCommand, path, targetPath.getOrElse(path).getParent.resolve("out"))
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
