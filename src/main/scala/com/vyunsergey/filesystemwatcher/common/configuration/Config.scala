package com.vyunsergey.filesystemwatcher.common.configuration

import cats.syntax.semigroup._
import tofu.logging.{DictLoggable, LogRenderer}
import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import java.io.File
import java.nio.file.{Path, Paths}
import Config._

final case class Config(
                         path: Path,
                         productMask: String,
                         fileMask: Map[String, FileMask],
                         transformer: Transformer
                       )

object Config {
  val defaultPath: Path = Paths.get(new File(getClass.getResource("/application.conf").toURI).toURI)

  final case class FileMask(dataFile: String, markerFile: String)
  final case class Transformer(mode: String, productPath: Path, jarPath: Path, command: String, options: Options)
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
        r.addString("productPath", a.productPath.toAbsolutePath.toString, i) |+|
        r.addString("jarPath", a.jarPath.toAbsolutePath.toString, i) |+|
        r.addString("command", a.command, i) |+|
        r.addString("options", optionsLoggable.logShow(a.options), i)
    }

    override def logShow(a: Transformer): String =
      s"Transformer(mode = '${a.mode}'" +
        s", productPath = '${a.productPath.toAbsolutePath.toString}'" +
        s", jarPath = '${a.jarPath.toAbsolutePath.toString}'" +
        s", command = '${a.command}'" +
        s", options = '${optionsLoggable.logShow(a.options)}')"
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
  implicit val sparkOptionsConverter: ConfigConvert[SparkOptions] = deriveConvert[SparkOptions]
  implicit val writerOptionsConverter: ConfigConvert[WriterOptions] = deriveConvert[WriterOptions]
  implicit val optionsConverter: ConfigConvert[Options] = deriveConvert[Options]
  implicit val transformerConverter: ConfigConvert[Transformer] = deriveConvert[Transformer]
  implicit val confConverter: ConfigConvert[Config] = deriveConvert[Config]
}
