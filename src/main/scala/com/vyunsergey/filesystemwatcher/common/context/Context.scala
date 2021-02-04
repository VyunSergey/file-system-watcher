package com.vyunsergey.filesystemwatcher.common.context

import cats.syntax.semigroup._
import com.vyunsergey.filesystemwatcher.common.configuration.Config
import tofu.logging.{DictLoggable, LogRenderer}

import java.nio.file.Path

final case class Context(
                          configPath: Path,
                          config: Config
                        )

object Context {

  implicit val argumentsLoggable: DictLoggable[Context] = new DictLoggable[Context] {
    override def fields[I, V, R, S](a: Context, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("configPath", a.configPath.toAbsolutePath.toString, i) |+|
        r.addString("config", Config.configLoggable.logShow(a.config), i)
    }

    override def logShow(a: Context): String =
      s"Context(configPath = '${a.configPath.toAbsolutePath.toString}'" +
        s", config = '${Config.configLoggable.logShow(a.config)}')"
  }

}
