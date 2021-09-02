package com.vyunsergey.filesystemwatcher.common.configuration

import cats.syntax.semigroup._
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File
import java.nio.file.{Path, Paths}
import Config._
import cats.Monad

import scala.util.{Failure, Success, Try}

final case class Config(
                         path: Path,
                         sdexPath: Path,
                         productMask: String,
                         transferFileMask: String,
                         transferArchive: String,
                         transferHash: String,
                         transferMarker: String,
                         csvMask: String,
                         zipMask: String,
                         fileMask: Map[String, FileMask],
                         transformer: Transformer
                       ) {
  def csvFileMasks[F[_]: Monad: Logging]: F[FileMask] = getFileMask("csv")
  def zipFileMasks[F[_]: Monad: Logging]: F[FileMask] = getFileMask("zip")
  def sparkMetaFileMasks[F[_]: Monad: Logging]: F[FileMask] = getFileMask("spark-meta")
  def sparkDataFileMasks[F[_]: Monad: Logging]: F[FileMask] = getFileMask("spark-data")
  def transferFileMasks[F[_]: Monad: Logging]: F[FileMask] = getFileMask("transfer")

  def getFileMask[F[_]: Monad: Logging](key: String): F[FileMask] = {
    for {
      mask <- Try(fileMask(key)) match {
        case Success(v) => v.pure[F]
        case Failure(exp) =>
          for {
            _ <- error"Can`t get '$key' files mask from Config: $this form Path: '${defaultPath.toAbsolutePath.toString}'. ${exp.getClass.getName}: ${exp.getMessage}"
          } yield throw exp
      }
    } yield mask
  }
}

object Config {
  lazy val defaultPath: Path = Paths.get(new File(getClass.getResource("/application.conf").toURI).toPath.toAbsolutePath.toString)

  final case class FileMask(dataFile: String, markerFile: String)
  final case class Transformer(mode: String, maxFilesCount: Int, maxFileSize: Long, productPath: Path, jarPath: Path, command: String, options: Options)
  final case class Options(spark: SparkOptions, writer: WriterOptions)
  final case class SparkOptions(spark: List[String])
  final case class WriterOptions(
                                  saveMode: Option[String],
                                  encoding: Option[String],
                                  header: Option[String],
                                  delimiter: Option[String],
                                  quote: Option[String],
                                  escape: Option[String],
                                  quoteMode: Option[String],
                                  nullValue: Option[String]
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

  implicit val sparkOptionsLoggable: DictLoggable[SparkOptions] = new DictLoggable[SparkOptions] {
    override def fields[I, V, R, S](a: SparkOptions, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("spark", a.spark.mkString("List(", ", ", ")"), i)
    }

    override def logShow(a: SparkOptions): String =
      s"SparkOptions(spark = '${a.spark.mkString("List(", ", ", ")")}')"
  }

  implicit val writerOptionsLoggable: DictLoggable[WriterOptions] = new DictLoggable[WriterOptions] {
    override def fields[I, V, R, S](a: WriterOptions, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("saveMode", a.saveMode.getOrElse("<None>"), i) |+|
        r.addString("encoding", a.encoding.getOrElse("<None>"), i) |+|
        r.addString("header", a.header.getOrElse("<None>"), i) |+|
        r.addString("delimiter", a.delimiter.getOrElse("<None>"), i) |+|
        r.addString("quote", a.quote.getOrElse("<None>"), i) |+|
        r.addString("escape", a.escape.getOrElse("<None>"), i) |+|
        r.addString("quoteMode", a.quoteMode.getOrElse("<None>"), i) |+|
        r.addString("nullValue", a.nullValue.getOrElse("<None>"), i)
    }

    override def logShow(a: WriterOptions): String =
      s"WriterOptions(saveMode = '${a.saveMode}'" +
        s", encoding = '${a.encoding}'" +
        s", header = '${a.header}'" +
        s", delimiter = '${a.delimiter}'" +
        s", quote = '${a.quote}'" +
        s", escape = '${a.escape}'" +
        s", quoteMode = '${a.quoteMode}'" +
        s", nullValue = '${a.nullValue}')"
  }

  implicit val optionsLoggable: DictLoggable[Options] = new DictLoggable[Options] {
    override def fields[I, V, R, S](a: Options, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("spark", sparkOptionsLoggable.logShow(a.spark), i) |+|
        r.addString("writer", writerOptionsLoggable.logShow(a.writer), i)
    }

    override def logShow(a: Options): String =
      s"Options(spark = '${sparkOptionsLoggable.logShow(a.spark)}'" +
        s", writer = '${writerOptionsLoggable.logShow(a.writer)}')"
  }

  implicit val transformerLoggable: DictLoggable[Transformer] = new DictLoggable[Transformer] {
    override def fields[I, V, R, S](a: Transformer, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("mode", a.mode, i) |+|
        r.addString("maxFilesCount", a.maxFilesCount.toString, i) |+|
        r.addString("maxFileSize", a.maxFileSize.toString, i) |+|
        r.addString("productPath", a.productPath.toAbsolutePath.toString, i) |+|
        r.addString("jarPath", a.jarPath.toAbsolutePath.toString, i) |+|
        r.addString("command", a.command, i) |+|
        r.addString("options", optionsLoggable.logShow(a.options), i)
    }

    override def logShow(a: Transformer): String =
      s"Transformer(mode = '${a.mode}'" +
        s", maxFilesCount = '${a.maxFilesCount.toString}'" +
        s", maxFileSize = '${a.maxFileSize.toString}'" +
        s", productPath = '${a.productPath.toAbsolutePath.toString}'" +
        s", jarPath = '${a.jarPath.toAbsolutePath.toString}'" +
        s", command = '${a.command}'" +
        s", options = '${optionsLoggable.logShow(a.options)}')"
  }

  implicit val configLoggable: DictLoggable[Config] = new DictLoggable[Config] {
    override def fields[I, V, R, S](a: Config, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
      r.addString("path", a.path.toAbsolutePath.toString, i) |+|
        r.addString("sdexPath", a.sdexPath.toAbsolutePath.toString, i) |+|
        r.addString("productMask", a.productMask, i) |+|
        r.addString("transferFileMask", a.transferFileMask, i) |+|
        r.addString("transferArchive", a.transferArchive, i) |+|
        r.addString("transferHash", a.transferHash, i) |+|
        r.addString("transferMarker", a.transferMarker, i) |+|
        r.addString("csvMask", a.csvMask, i) |+|
        r.addString("zipMask", a.zipMask, i) |+|
        r.addString("fileMask", a.fileMask.view.mapValues(fileMaskLoggable.logShow).mkString("[", ",", "]"), i) |+|
        r.addString("transformer", transformerLoggable.logShow(a.transformer), i)
    }

    override def logShow(a: Config): String =
      s"Config(path = '${a.path.toAbsolutePath.toString}'" +
        s", sdexPath = '${a.sdexPath.toAbsolutePath.toString}'" +
        s", productMask = '${a.productMask}'" +
        s", transferFileMask = '${a.transferFileMask}'" +
        s", transferArchive = '${a.transferArchive}'" +
        s", transferHash = '${a.transferHash}'" +
        s", transferMarker = '${a.transferMarker}'" +
        s", csvMask = '${a.csvMask}'" +
        s", zipMask = '${a.zipMask}'" +
        s", fileMask = '${a.fileMask.view.mapValues(fileMaskLoggable.logShow).mkString("[", ",", "]")}'" +
        s", transformer = '${transformerLoggable.logShow(a.transformer)}')"
  }

  implicit val fileMaskConverter: ConfigConvert[FileMask] = deriveConvert[FileMask]
  implicit val sparkOptionsConverter: ConfigConvert[SparkOptions] = deriveConvert[SparkOptions]
  implicit val writerOptionsConverter: ConfigConvert[WriterOptions] = deriveConvert[WriterOptions]
  implicit val optionsConverter: ConfigConvert[Options] = deriveConvert[Options]
  implicit val transformerConverter: ConfigConvert[Transformer] = deriveConvert[Transformer]
  implicit val confConverter: ConfigConvert[Config] = deriveConvert[Config]
}
