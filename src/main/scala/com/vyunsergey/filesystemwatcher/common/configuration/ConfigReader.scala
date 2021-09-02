package com.vyunsergey.filesystemwatcher.common.configuration

import cats.Monad
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._
import pureconfig.ConfigSource
import cats.effect.Resource

import java.nio.file.Path

class ConfigReader[F[_]: Monad: Logging] {
  def read(path: Path): Resource[F, Config] = {
    for {
      config <- Resource.eval(
        info"Reading Config" as {
          ConfigSource.file(path).withFallback(ConfigSource.default).loadOrThrow[Config]
        }
      )
      _ <- Resource.eval(info"$config")
    } yield config
  }
}

object ConfigReader {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, ConfigReader[F]] =
    Resource.eval(logs.forService[ConfigReader[F]].map(implicit l => new ConfigReader[F]))
}
