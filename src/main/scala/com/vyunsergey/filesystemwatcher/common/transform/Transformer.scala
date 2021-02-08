package com.vyunsergey.filesystemwatcher.common.transform

import cats.Monad
import cats.effect.Resource
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.sys.process.Process

class Transformer[F[_]: Monad: Logging] {
  def createCommand(mode: String,
                    jarPath: Path,
                    options: String,
                    commandMask: String): F[String] = {
    for {
      command <- info"Creating Transformer Command" as {
        commandMask
          .replaceFirst("<OPTIONS>", options)
          .replaceFirst("<JAR_PATH>", jarPath.toAbsolutePath.toString.replace("\\", "/"))
          .replaceFirst("<MODE>", mode)
      }
      _ <- info"Transformer Options: $options"
      _ <- info"Transformer .JAR Path: '${jarPath.toAbsolutePath.toString}'"
      _ <- info"Transformer Mode: $mode"
      _ <- info"Transformer Command: $command"
    } yield command
  }

  def exec(command: String,
           sourcePath: Path,
           targetPath: Path): F[Process] = {
    for {
      command <- info"Adding Source Path and Target Path to Command" as {
        command
          .replaceFirst("<SRC_PATH>", sourcePath.toAbsolutePath.toString.replace("\\", "/"))
          .replaceFirst("<TGT_PATH>", targetPath.toAbsolutePath.toString.replace("\\", "/"))
      }
      _ <- info"Transformer Source Path: '${sourcePath.toAbsolutePath.toString}'"
      _ <- info"Transformer Target Path: '${targetPath.toAbsolutePath.toString}'"
      _ <- info"Running Transformer Command: '$command'"
    } yield {
      Process(command).run()
    }
  }
}

object Transformer {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, Transformer[F]] =
    Resource.liftF(logs.forService[Transformer[F]].map(implicit l => new Transformer[F]))
}
