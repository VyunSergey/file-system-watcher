package com.vyunsergey.filesystemwatcher.common.context

import cats.Monad
import com.vyunsergey.filesystemwatcher.common.arguments.Arguments
import com.vyunsergey.filesystemwatcher.common.configuration.Config
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._
import cats.effect.Resource

class ContextReader[F[_]: Monad: Logging] {
  def read(arguments: Arguments, config: Config): Resource[F, Context] = {
    for {
      context <- Resource.liftF(
        info"Reading Context" as {
          Context(arguments.configPath.getOrElse(Config.defaultPath), config)
        }
      )
      _ <- Resource.liftF(info"$context")
    } yield context
  }
}

object ContextReader {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, ContextReader[F]] = {
    Resource.liftF(logs.forService[ContextReader[F]].map(implicit l => new ContextReader[F]))
  }
}
