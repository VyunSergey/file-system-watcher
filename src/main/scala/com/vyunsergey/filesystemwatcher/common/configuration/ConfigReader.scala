package com.vyunsergey.filesystemwatcher.common.configuration

import java.io.File
import java.nio.file.{Path, Paths}

import cats.Monad
import cats.implicits._
import com.vyunsergey.filesystemwatcher.common.configuration.ConfigReader._
import pureconfig.{ConfigConvert, ConfigSource}

trait ConfigReader[F[_]] {
  def read(path: Path)(implicit M: Monad[F]): F[Conf] = {
    ConfigSource.file(path).loadOrThrow[Conf].pure[F]
  }
  def convertPath(pathName: String)(implicit M: Monad[F]): F[Path] = {
    Paths.get(new File(pathName).toURI).pure[F]
  }
}

object ConfigReader {
  import pureconfig.generic.semiauto.deriveConvert

  def apply[F[_]]: ConfigReader[F] = new ConfigReader[F] {}

  final case class Conf(
                         path: Path,
                         fileMask: String,
                         markerFileMask: String,
                         transformerKeyValMode: String,
                         transformerKeyValPath: String,
                         transformerKeyValOptions: String,
                         transformerKeyValExecCommand: String
                       )

  val configPathDefault: Path = Paths.get(new File(getClass.getResource("/application.conf").toURI).toURI)

  implicit val confConverter: ConfigConvert[Conf] = deriveConvert[Conf]
}
