package com.vyunsergey.filesystemwatcher.common.execute

import cats.Monad
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.sys.process.Process

class Executor[F[_]: Monad: Logging] {
  def createExecutorCommand(transformerKeyValMode: String,
                            transformerKeyValPath: String,
                            transformerKeyValOptions: String,
                            transformerKeyValExecCommand: String): F[String] = {
    info"Creating Executor Command" as {
      transformerKeyValExecCommand
        .replaceFirst("<OPTIONS>", transformerKeyValOptions)
        .replaceFirst("<JAR_PATH>", transformerKeyValPath)
        .replaceFirst("<MODE>", transformerKeyValMode)
    }
  }

  def exec(commandMask: String,
           sourcePath: Path,
           targetPath: Path): F[Process] = {
    for {
      command <- info"Adding Source Path and Target Path to Command" as {
        commandMask
          .replaceFirst("<SRC_PATH>", sourcePath.toAbsolutePath.toString.replace("\\", "/"))
          .replaceFirst("<TGT_PATH>", targetPath.toAbsolutePath.toString.replace("\\", "/"))
      }
      _ <- info"Transformer Executor Command Mask: '$commandMask'"
      _ <- info"Transformer Source Path: '${sourcePath.toAbsolutePath.toString}'"
      _ <- info"Transformer Target Path: '${targetPath.toAbsolutePath.toString}'"
      _ <- info"Transformer Executor Command: '$command'"
      _ <- info"Running Executor Command: '$command'"
    } yield {
      Process(command).run()
    }
  }
}

object Executor {
  def make[F[_]: Monad](logs: Logs[F, F]): F[Executor[F]] =
    logs.forService[Executor[F]].map(implicit l => new Executor[F])
}
