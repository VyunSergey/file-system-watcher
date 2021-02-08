package com.vyunsergey.filesystemwatcher.common.configuration

import cats.syntax.semigroup._
import tofu.logging.{DictLoggable, LogRenderer}
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File
import java.nio.file.{Path, Paths}

final case class Config(
                         path: Path,
                         productMask: String,
                         fileMask: Map[String, Config.FileMask],
                         transformer: Config.Transformer
                       )

object Config {

  val pathDefault: Path = Paths.get(new File(getClass.getResource("/application.conf").toURI).toURI)

  final case class FileMask(
                             dataFile: String,
                             markerFile: String
                           )

  final case class Transformer(
                                mode: String,
                                jarPath: Path,
                                command: String,
                                options: Map[String, List[String]]
                              )

  implicit val fileMaskLoggable: DictLoggable[FileMask] = new DictLoggable[FileMask] {
    override def fields[I, V, R, S](a: FileMask, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("dataFile", a.dataFile, i) |+|
        r.addString("markerFile", a.markerFile, i)
    }

    override def logShow(a: FileMask): String =
      s"FileMask(dataFile = '${a.dataFile}'" +
        s", markerFile = '${a.markerFile}')"
  }

  implicit val transformerLoggable: DictLoggable[Transformer] = new DictLoggable[Transformer] {
    override def fields[I, V, R, S](a: Transformer, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("mode", a.mode, i) |+|
        r.addString("jarPath", a.jarPath.toAbsolutePath.toString, i) |+|
        r.addString("command", a.command, i) |+|
        r.addString("options", a.options.view.mapValues(_.mkString("[", ",", "]")).mkString("[", ",", "]"), i)
    }

    override def logShow(a: Transformer): String =
      s"Transformer(mode = '${a.mode}'" +
        s", jarPath = '${a.jarPath.toAbsolutePath.toString}'" +
        s", command = '${a.command}'" +
        s", options = '${a.options.view.mapValues(_.mkString("[", ",", "]")).mkString("[", ",", "]")}')"
  }

  implicit val configLoggable: DictLoggable[Config] = new DictLoggable[Config] {
    override def fields[I, V, R, S](a: Config, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("path", a.path.toAbsolutePath.toString, i) |+|
        r.addString("productMask", a.productMask, i) |+|
        r.addString("fileMask", a.fileMask.view.mapValues(fileMaskLoggable.logShow).mkString("[", ",", "]"), i) |+|
        r.addString("transformer", transformerLoggable.logShow(a.transformer), i)
    }

    override def logShow(a: Config): String =
      s"Config(path = '${a.path.toAbsolutePath.toString}'" +
        s", productMask = '${a.productMask}'" +
        s", fileMask = '${a.fileMask.view.mapValues(fileMaskLoggable.logShow).mkString("[", ",", "]")}'" +
        s", transformer = '${transformerLoggable.logShow(a.transformer)}')"
  }

  implicit val fileMaskConverter: ConfigConvert[FileMask] = deriveConvert[FileMask]
  implicit val transformerConverter: ConfigConvert[Transformer] = deriveConvert[Transformer]
  implicit val confConverter: ConfigConvert[Config] = deriveConvert[Config]
}
