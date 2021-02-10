package com.vyunsergey.filesystemwatcher.common.transform

import cats.Monad
import cats.effect.Resource
import com.vyunsergey.filesystemwatcher.common.context.Context
import com.vyunsergey.filesystemwatcher.common.transform.TransformerConfig.TransformerReaderConfig
import com.vyunsergey.filesystemwatcher.processor.file.FileProcessor
import io.circe.parser._
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
      dataOp <- fileProcessor.readFile(path)
      readerConfig <- dataOp match {
        case Some(data) =>
          for {
            config <- decode[TransformerReaderConfig](data) match {
              case Left(err) =>
                error"Can`t parse data as JSON for Transformer Config form Path: '${path.toAbsolutePath.toString}'. ${err.getClass.getName}: ${err.getMessage}" as None
              case Right(conf) => Some(conf).pure[F]
            }
          } yield config
        case None => error"Can`t read file for Transformer Config form Path: '${path.toAbsolutePath.toString}'" as None
      }
      _ <- readerConfig.map(c => info"$c").getOrElse(().pure[F])
    } yield readerConfig
  }
}

object TransformerConfigReader {
  def apply[F[_]: Monad: FileProcessor](context: Context, logs: Logs[F, F]): Resource[F, TransformerConfigReader[F]] =
    Resource.liftF(logs.forService[TransformerConfigReader[F]].map(implicit l => new TransformerConfigReader[F](context: Context)))
}
