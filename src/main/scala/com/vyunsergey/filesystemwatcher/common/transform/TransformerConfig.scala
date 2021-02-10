package com.vyunsergey.filesystemwatcher.common.transform

import cats.syntax.semigroup._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import tofu.logging._
import TransformerConfig._
import com.vyunsergey.filesystemwatcher.common.configuration.Config

final case class TransformerConfig(
                                    spark: TransformerSparkConfig,
                                    reader: TransformerReaderConfig,
                                    writer: TransformerWriterConfig
                                  ) {
  def toSparkOptions: String = spark.toSparkOptions
  def toReaderOptions(withInferSchema: Boolean = true): String = reader.toReaderOptions(withInferSchema)
  def toWriterOptions: String = writer.toWriterOptions
  def toOptions(withInferSchema: Boolean = true): String = {
    List(toSparkOptions, toReaderOptions(withInferSchema), toWriterOptions).mkString(" ")
  }
}

object TransformerConfig {
  def make(reader: TransformerReaderConfig, config: Config): TransformerConfig = {
    val sparkOptions = config.transformer.options.spark
    val writerOptions = config.transformer.options.writer

    TransformerConfig(
      spark = TransformerSparkConfig(sparkOptions.spark),
      reader,
      writer = TransformerWriterConfig(
        writerOptions.saveMode,
        writerOptions.encoding,
        writerOptions.header,
        writerOptions.delimiter,
        writerOptions.quote,
        writerOptions.escape,
        writerOptions.quoteMode,
        writerOptions.nullValue
      )
    )
  }

  final case class TransformerSparkConfig(
                                           spark: List[String]
                                         ) {
    def toSparkOptions: String =
      spark.map(x => if (x.startsWith("-D")) x else "-D" + x).mkString(" ")
  }

  final case class TransformerReaderConfig(
                                            encoding: Option[String],
                                            header: Option[String],
                                            delimiter: Option[String],
                                            quote: Option[String],
                                            escape: Option[String],
                                            linesEnding: Option[String]
                                          ) {
    def toReaderOptions(withInferSchema: Boolean = true): String = {
      List(
        encoding.map(enc => s"-Dreader.csv.charset='$enc'"),
        header.map(hdr => s"-Dreader.csv.header='$hdr'"),
        delimiter.flatMap(del =>
          if (del.length > 1 || del == ",") None
          else if (del == "'") Some(s"""-Dreader.csv.delimiter="$del"""")
          else Some(s"-Dreader.csv.delimiter='$del'")),
        quote.flatMap(qot =>
          if (qot.length > 1 || qot == "\"") None
          else if (qot == "'") Some(s"""-Dreader.csv.quote="$qot"""")
          else Some(s"-Dreader.csv.quote='$qot'")),
        escape.flatMap(esc =>
          if (esc.length > 1 || esc == "\\") None
          else if (esc == "'") Some(s"""-Dreader.csv.escape="$esc"""")
          else Some(s"-Dreader.csv.escape='$esc'")),
        if (withInferSchema) Some("-Dreader.csv.inferSchema='true'") else None
      ).collect{case Some(x) => x}.mkString(" ")
    }
  }

  final case class TransformerWriterConfig(
                                            saveMode: Option[String],
                                            encoding: Option[String],
                                            header: Option[String],
                                            delimiter: Option[String],
                                            quote: Option[String],
                                            escape: Option[String],
                                            quoteMode: Option[String],
                                            nullValue: Option[String]
                                          ) {
    def toWriterOptions: String = {
      List(
        saveMode.map(svm => s"-Dwriter.saveMode='$svm'"),
        encoding.map(enc => s"-Dwriter.csv.charset='$enc'"),
        header.map(hdr => s"-Dwriter.csv.header='$hdr'"),
        delimiter.flatMap(del =>
          if (del.length > 1 || del == ",") None
          else if (del == "'") Some(s"""-Dwriter.csv.delimiter="$del"""")
          else Some(s"-Dwriter.csv.delimiter='$del'")),
        quote.flatMap(qot =>
          if (qot.length > 1 || qot == "\"") None
          else if (qot == "'") Some(s"""-Dwriter.csv.quote="$qot"""")
          else Some(s"-Dwriter.csv.quote='$qot'")),
        escape.flatMap(esc =>
          if (esc.length > 1 || esc == "\\") None
          else if (esc == "'") Some(s"""-Dwriter.csv.escape="$esc"""")
          else Some(s"-Dwriter.csv.escape='$esc'")),
        quoteMode.map(qtm => s"-Dwriter.csv.quoteMode='$qtm'"),
        nullValue.map(nlv => s"-Dwriter.csv.nullValue='$nlv'")
      ).collect{case Some(x) => x}.mkString(" ")
    }
  }

  implicit val transformerSparkConfigLoggable: DictLoggable[TransformerSparkConfig] =
    new DictLoggable[TransformerSparkConfig] {
      override def fields[I, V, R, S](a: TransformerSparkConfig, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
        r.addString("spark", a.spark.mkString("List(", ", ", ")"), i)
      }

      override def logShow(a: TransformerSparkConfig): String =
        s"TransformerSparkConfig(spark = '${a.spark.mkString("List(", ", ", ")")}')"
    }

  implicit val transformerReaderConfigLoggable: DictLoggable[TransformerReaderConfig] =
    new DictLoggable[TransformerReaderConfig] {
      override def fields[I, V, R, S](a: TransformerReaderConfig, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
        r.addString("encoding", a.encoding.getOrElse("<None>"), i) |+|
          r.addString("header", a.header.getOrElse("<None>"), i) |+|
          r.addString("delimiter", a.delimiter.getOrElse("<None>"), i) |+|
          r.addString("quote", a.quote.getOrElse("<None>"), i) |+|
          r.addString("escape", a.escape.getOrElse("<None>"), i) |+|
          r.addString("linesEnding", a.linesEnding.getOrElse("<None>"), i)
      }

      override def logShow(a: TransformerReaderConfig): String =
        s"TransformerReaderConfig(encoding = '${a.encoding}'" +
          s", header = '${a.header}'" +
          s", delimiter = '${a.delimiter}'" +
          s", quote = '${a.quote}'" +
          s", escape = '${a.escape}'" +
          s", linesEnding = '${a.linesEnding}')"
    }

  implicit val transformerWriterConfigLoggable: DictLoggable[TransformerWriterConfig] =
    new DictLoggable[TransformerWriterConfig] {
      override def fields[I, V, R, S](a: TransformerWriterConfig, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
        r.addString("saveMode", a.saveMode.getOrElse("<None>"), i) |+|
          r.addString("encoding", a.encoding.getOrElse("<None>"), i) |+|
          r.addString("header", a.header.getOrElse("<None>"), i) |+|
          r.addString("delimiter", a.delimiter.getOrElse("<None>"), i) |+|
          r.addString("quote", a.quote.getOrElse("<None>"), i) |+|
          r.addString("escape", a.escape.getOrElse("<None>"), i) |+|
          r.addString("quoteMode", a.quoteMode.getOrElse("<None>"), i) |+|
          r.addString("nullValue", a.nullValue.getOrElse("<None>"), i)
      }

      override def logShow(a: TransformerWriterConfig): String =
        s"TransformerWriterConfig(saveMode = '${a.saveMode}'" +
          s", encoding = '${a.encoding}'" +
          s", header = '${a.header}'" +
          s", delimiter = '${a.delimiter}'" +
          s", quote = '${a.quote}'" +
          s", escape = '${a.escape}'" +
          s", quoteMode = '${a.quoteMode}'" +
          s", nullValue = '${a.nullValue}')"
    }

  implicit val transformerConfigLoggable: DictLoggable[TransformerConfig] =
    new DictLoggable[TransformerConfig] {
      override def fields[I, V, R, S](a: TransformerConfig, i: I)(implicit r: LogRenderer[I, V, R, S]): R = {
        r.addString("spark", transformerSparkConfigLoggable.logShow(a.spark), i) |+|
          r.addString("reader", transformerReaderConfigLoggable.logShow(a.reader), i) |+|
          r.addString("writer", transformerWriterConfigLoggable.logShow(a.writer), i)
      }

      override def logShow(a: TransformerConfig): String =
        s"TransformerConfig(spark = '${transformerSparkConfigLoggable.logShow(a.spark)}'" +
          s", reader = '${transformerReaderConfigLoggable.logShow(a.reader)}'" +
          s", writer = '${transformerWriterConfigLoggable.logShow(a.writer)}')"
    }

  implicit val transformerSparkConfigDecoder: Decoder[TransformerSparkConfig] = deriveDecoder[TransformerSparkConfig]
  implicit val transformerSparkConfigEncoder: Encoder[TransformerSparkConfig] = deriveEncoder[TransformerSparkConfig]

  implicit val transformerReaderConfigDecoder: Decoder[TransformerReaderConfig] = deriveDecoder[TransformerReaderConfig]
  implicit val transformerReaderConfigEncoder: Encoder[TransformerReaderConfig] = deriveEncoder[TransformerReaderConfig]

  implicit val transformerWriterConfigDecoder: Decoder[TransformerWriterConfig] = deriveDecoder[TransformerWriterConfig]
  implicit val transformerWriterConfigEncoder: Encoder[TransformerWriterConfig] = deriveEncoder[TransformerWriterConfig]

  implicit val transformerConfigDecoder: Decoder[TransformerConfig] = deriveDecoder[TransformerConfig]
  implicit val transformerConfigEncoder: Encoder[TransformerConfig] = deriveEncoder[TransformerConfig]
}
