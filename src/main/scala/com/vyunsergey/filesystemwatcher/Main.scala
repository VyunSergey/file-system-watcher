package com.vyunsergey.filesystemwatcher

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import tofu.logging.Logs
import tofu.syntax.monadic._
import com.vyunsergey.filesystemwatcher.common.arguments.ArgumentsReader
import com.vyunsergey.filesystemwatcher.common.configuration.{Config, ConfigReader}
import com.vyunsergey.filesystemwatcher.common.context.ContextReader
import com.vyunsergey.filesystemwatcher.common.transform.{Transformer, TransformerConfigReader}
import com.vyunsergey.filesystemwatcher.processor.file.{CsvFileProcessor, DataFileProcessor, FileProcessor, MarkerFileProcessor, SparkFileProcessor, TransferFileProcessor, ZipFileProcessor}
import com.vyunsergey.filesystemwatcher.processor.event.EventProcessor
import com.vyunsergey.filesystemwatcher.watcher.FileSystemWatcher

import scala.concurrent.ExecutionContext
import java.util.concurrent.ForkJoinPool

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    val syncLogs: Logs[IO, IO] = Logs.sync[IO, IO]
    (for {
      argumentsReader <- ArgumentsReader[IO](syncLogs)
      arguments <- argumentsReader.read(args)
      configReader <- ConfigReader[IO](syncLogs)
      configPath = arguments.configPath.getOrElse(Config.defaultPath)
      config <- configReader.read(configPath)
      contextReader <- ContextReader[IO](syncLogs)
      context <- contextReader.read(arguments, config)
      executionContext = ExecutionContext.fromExecutor(new ForkJoinPool(8))
      blocker = Blocker.liftExecutionContext(executionContext)
      watcher <- FileSystemWatcher[IO](blocker, syncLogs)
      implicit0(fileProcessor: FileProcessor[IO]) <- FileProcessor[IO](syncLogs)
      implicit0(transformerConfigReader: TransformerConfigReader[IO]) <- TransformerConfigReader[IO](context, syncLogs)
      implicit0(transformer: Transformer[IO]) <- Transformer[IO](context, syncLogs)
      implicit0(csvFileProcessor: CsvFileProcessor[IO]) <- CsvFileProcessor[IO](context, syncLogs)
      implicit0(zipFileProcessor: ZipFileProcessor[IO]) <- ZipFileProcessor[IO](context, syncLogs)
      implicit0(sparkFileProcessor: SparkFileProcessor[IO]) <- SparkFileProcessor[IO](context, syncLogs)
      implicit0(transferFileProcessor: TransferFileProcessor[IO]) <- TransferFileProcessor[IO](context, syncLogs)
      implicit0(dataFileProcessor: DataFileProcessor[IO]) <- DataFileProcessor[IO](context, syncLogs)
      implicit0(markerFileProcessor: MarkerFileProcessor[IO]) <- MarkerFileProcessor[IO](context, syncLogs)
      eventProcessor <- EventProcessor[IO](syncLogs)
      stream <- watcher.watch(context.config.path)
      processedStream <- Resource.eval(stream.evalMap(event => eventProcessor.process(event)).pure[IO])
    } yield {
      processedStream
    }).use(_.compile.drain.as(ExitCode.Success))
  }
}
