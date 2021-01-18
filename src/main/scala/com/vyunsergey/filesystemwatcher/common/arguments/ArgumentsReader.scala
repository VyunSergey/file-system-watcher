package com.vyunsergey.filesystemwatcher.common.arguments

import cats.Monad
import cats.implicits._
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ArgumentsReader(args: Seq[String]) extends ScallopConf(args) {
  val path: ScallopOption[String] = opt[String](name = "path", validate = _.nonEmpty)
  val fileMask: ScallopOption[String] = opt[String](name = "file-mask", validate = _.nonEmpty)
  val markerFileMask: ScallopOption[String] = opt[String](name = "marker-file-mask", validate = _.nonEmpty)
  val configPath: ScallopOption[String] = opt[String](name = "config-path", validate = _.nonEmpty)
  verify()
}

object ArgumentsReader {
  def apply[F[_]](args: Seq[String])(implicit M: Monad[F]): F[ArgumentsReader] = {
    new ArgumentsReader(args).pure[F]
  }
}
