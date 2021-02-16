package com.vyunsergey.filesystemwatcher.common.transform

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path
import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

class Transformer[F[_]: Monad: Logging: TransformerConfigReader](context: Context) {
  def createConfig(productId: String): F[Option[TransformerConfig]] = {
    for {
      transformerConfigReader <- info"Finding Transformer Reader Config for product: $productId" as {
        implicitly[TransformerConfigReader[F]]
      }
      transformerReaderConfigOp <- transformerConfigReader.readReaderConfig(productId)
      transformerConfig <- transformerReaderConfigOp match {
        case Some(transformerReaderConfig) => info"Creating Transformer Config for product: $productId" as {
          Some(TransformerConfig.make(transformerReaderConfig, context.config))
        }
        case None => error"Not Found Transformer Reader Config for product: $productId" as None
      }
    } yield transformerConfig
  }

  def createCommand(mode: String,
                    jarPath: Path,
                    config: TransformerConfig,
                    commandMask: String): F[String] = {
    for {
      command <- info"Creating Transformer Command" as {
        commandMask
          .replaceFirst("<OPTIONS>", config.toOptions())
          .replaceFirst("<JAR_PATH>", jarPath.toAbsolutePath.toString.replace("\\", "/"))
          .replaceFirst("<MODE>", mode)
      }
      _ <- info"Transformer Config: $config"
      _ <- info"Transformer .JAR Path: '${jarPath.toAbsolutePath.toString}'"
      _ <- info"Transformer Mode: $mode"
      _ <- info"Transformer Command: $command"
    } yield command
  }

  def exec(command: String,
           numParts: Int,
           sourcePath: Path,
           targetPath: Path): F[Unit] = {
    for {
      command <- info"Adding Source Path and Target Path to Command" as {
        command
          .replaceFirst("<NUM_PARTS>", numParts.toString)
          .replaceFirst("<SRC_PATH>", sourcePath.toAbsolutePath.toString.replace("\\", "/"))
          .replaceFirst("<TGT_PATH>", targetPath.toAbsolutePath.toString.replace("\\", "/"))
      }
      _ <- info"Transformer Source Path: '${sourcePath.toAbsolutePath.toString}'"
      _ <- info"Transformer Target Path: '${targetPath.toAbsolutePath.toString}'"
      processTry <- info"Running Transformer Command: '$command'" as {
        Try(Process(command).run())
      }
      _ <- processTry match {
        case Success(_) => info"Transforming to Key-Value..."
        case Failure(exp) => error"Can`t run Transformer Command: '$command'. ${exp.getClass.getName}: ${exp.getMessage}"
      }
    } yield ()
  }
}

object Transformer {
  def apply[F[_]: Monad: TransformerConfigReader](context: Context, logs: Logs[F, F]): Resource[F, Transformer[F]] =
    Resource.liftF(logs.forService[Transformer[F]].map(implicit l => new Transformer[F](context: Context)))
}
