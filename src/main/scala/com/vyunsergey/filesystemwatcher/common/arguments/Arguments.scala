package com.vyunsergey.filesystemwatcher.common.arguments

import cats.syntax.semigroup._
import tofu.logging.{DictLoggable, LogRenderer}

import java.nio.file.Path

final case class Arguments(
                            path: Option[Path],
                            configPath: Option[Path]
                          )

object Arguments {

  implicit val argumentsLoggable: DictLoggable[Arguments] = new DictLoggable[Arguments] {
    override def fields[I, V, R, S](a: Arguments, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("path", a.path.map(_.toAbsolutePath.toString).getOrElse(""), i) |+|
        r.addString("configPath", a.configPath.map(_.toString).getOrElse(""), i)
    }

    override def logShow(a: Arguments): String =
      s"Arguments(path = '${a.path.map(_.toAbsolutePath.toString).getOrElse("")}'" +
        s", configPath = '${a.configPath.map(_.toString).getOrElse("")}')"
  }

}
