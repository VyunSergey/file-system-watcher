package com.vyunsergey.filesystemwatcher.common.transform

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.TransformerConfig.TransformerReaderConfig
import com.vyunsergey.filesystemwatcher.processor.file.FileProcessor
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.Path

class TransformerConfigReader[F[_]: Monad: Logging: FileProcessor](context: Context) {
  def readReaderConfig(productId: String): F[Option[TransformerReaderConfig]] = {
    for {
      productsConfigPath <- info"Getting Transformer products Path from Config: ${context.config.transformer}" as {
        context.config.transformer.productPath
      }
      path = productsConfigPath.resolve(s"$productId.json")
      config <- readReaderConfig(path)
    } yield config
  }

  def readReaderConfig(path: Path): F[Option[TransformerReaderConfig]] = {
    for {
      fileProcessor <- info"Reading Transformer Config from Path: '${path.toAbsolutePath.toString}'" as {
        implicitly[FileProcessor[F]]
      }
      readerConfigOp <- fileProcessor.readJson[TransformerReaderConfig](path)
      _ <- readerConfigOp match {
        case Some(_) =>
          info"Finish read file and parse data as JSON for Transformer Config form Path: '${path.toAbsolutePath.toString}'"
        case None =>
          error"Can`t read file and parse data as JSON for Transformer Config form Path: '${path.toAbsolutePath.toString}'"
      }
      _ <- readerConfigOp.map(conf => info"$conf").getOrElse(().pure[F])
    } yield readerConfigOp
  }
}

object TransformerConfigReader {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, TransformerConfigReader[F]] =
    Resource.liftF(logs.forService[TransformerConfigReader[F]].map(implicit l => new TransformerConfigReader[F](context)))
}
