package com.vyunsergey.filesystemwatcher.common.arguments

import cats.Monad
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._
import cats.effect.Resource

class ArgumentsReader[F[_]: Monad: Logging] {
  def read(args: Seq[String]): Resource[F, Arguments] = {
    for {
      arguments <- Resource.liftF(
        info"Reading Arguments" as {
          val argumentsOps: ArgumentsOps = new ArgumentsOps(args)
          Arguments(
            argumentsOps.path.toOption,
            argumentsOps.configPath.toOption
          )
        }
      )
      _ <- Resource.liftF(info"$arguments")
    } yield arguments
  }
}

object ArgumentsReader {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, ArgumentsReader[F]] = {
    Resource.liftF(logs.forService[ArgumentsReader[F]].map(implicit l => new ArgumentsReader[F]))
  }
}
