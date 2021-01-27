package com.vyunsergey.filesystemwatcher.common

import cats.effect.Async
import cats.implicits._

import java.nio.file.Path
import scala.sys.process.Process

trait TransformerExecutor[F[_]] {
  def createExecutorCommand(transformerKeyValMode: String,
                            transformerKeyValPath: String,
                            transformerKeyValOptions: String,
                            transformerKeyValExecCommand: String)(implicit AF: Async[F]): F[String] = {
    transformerKeyValExecCommand
      .replaceFirst("<OPTIONS>", transformerKeyValOptions)
      .replaceFirst("<JAR_PATH>", transformerKeyValPath)
      .replaceFirst("<MODE>", transformerKeyValMode)
      .pure[F]
  }

  def exec(executorCommand: String,
           sourcePath: Path,
           targetPath: Path)(implicit AF: Async[F]): F[Process] = {
    AF.delay{
      val execCommand = executorCommand
        .replaceFirst("<SRC_PATH>", sourcePath.toString.replace("\\", "/"))
        .replaceFirst("<TGT_PATH>", targetPath.toString.replace("\\", "/"))

      println(s"Transformer Exec Mask: '$executorCommand'")
      println(s"Transformer Source Path: '${sourcePath.toString}'")
      println(s"Transformer Target Path: '${targetPath.toString}'")
      println(s"Transformer Exec Command: '$execCommand'")

      Process(execCommand).run()
    }
  }

}

object TransformerExecutor {
  def apply[F[_]](implicit AF: Async[F]): TransformerExecutor[F] = new TransformerExecutor[F] {}
}
