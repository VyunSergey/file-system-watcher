package com.vyunsergey.filesystemwatcher

import cats.effect.{Blocker, ExitCode, IO, IOApp, Resource}
import tofu.logging.Logs
import tofu.syntax.monadic._
import com.vyunsergey.filesystemwatcher.common.arguments.ArgumentsReader
import com.vyunsergey.filesystemwatcher.common.configuration.{Config, ConfigReader}
import com.vyunsergey.filesystemwatcher.common.context.ContextReader
import com.vyunsergey.filesystemwatcher.processor.file.{CsvFileProcessor, DataFileProcessor, FileProcessor, MarkerFileProcessor, ZipFileProcessor}
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
      configPath = arguments.configPath.getOrElse(Config.pathDefault)
      config <- configReader.read(configPath)
      contextReader <- ContextReader[IO](syncLogs)
      context <- contextReader.read(arguments, config)
      executionContext = ExecutionContext.fromExecutor(new ForkJoinPool(8))
      blocker = Blocker.liftExecutionContext(executionContext)
      watcher <- FileSystemWatcher[IO](blocker, syncLogs)
      implicit0(fileProcessor: FileProcessor[IO]) <- FileProcessor[IO](syncLogs)
      implicit0(csvFileProcessor: CsvFileProcessor[IO]) <- CsvFileProcessor[IO](context, syncLogs)
      implicit0(zipFileProcessor: ZipFileProcessor[IO]) <- ZipFileProcessor[IO](context, syncLogs)
      implicit0(dataFileProcessor: DataFileProcessor[IO]) <- DataFileProcessor[IO](context, syncLogs)
      implicit0(markerFileProcessor: MarkerFileProcessor[IO]) <- MarkerFileProcessor[IO](context, syncLogs)
      eventProcessor <- EventProcessor[IO](syncLogs)
      stream <- watcher.watch(context.config.path)
      processedStream <- Resource.liftF(stream.evalMap(event => eventProcessor.process(event)).pure[IO])
    } yield {
      processedStream
    }).use(_.compile.drain.as(ExitCode.Success))
  }
/*
  def process[F[_]](name: String,
                    path: Path,
                    dataFileMask: String,
                    markerFileMask: String,
                    transformerCommand: String)(implicit AF: Async[F]): F[Unit] =
    for {
      _ <- println(s"Got File: '$name' in Path: '${path.toAbsolutePath.toString}'").pure[F]
      (isExists, isFile) = (JFiles.exists(path), JFiles.isRegularFile(path))
      (isMarkerFile, isDataFile) = (name.matches(markerFileMask.r.regex), name.matches(dataFileMask.r.regex))
      _ <- AF.pure {
        println(s"Path is ${if (isFile) "File" else "Directory"}")
        println(s"Path exists: $isExists")
        println(s"Path file name: '$name'")
        println(s"Path file name '$name' is " +
          s"${if (isMarkerFile) "Marker File" else if (isDataFile) "Data File" else "Other"}")
      }
      _ <- if (isExists && isMarkerFile) {
        processMarkerFile[F](name, path, dataFileMask, markerFileMask, transformerCommand)
        } else ().pure[F]
    } yield ()

  def findPredicate[F[_]](regexp: Regex)(implicit AF: Async[F]): F[BiPredicate[Path, BasicFileAttributes]] =
    new BiPredicate[Path, BasicFileAttributes] {
      def test(t: Path, u: BasicFileAttributes): Boolean = {
        val matches: Boolean = t.getFileName.toString.matches(regexp.regex)

        println(s"  got path: '$t'")
        println(s"  path is file: ${u.isRegularFile}")
        println(s"  path matches regexp '${regexp.regex}': $matches")

        matches && u.isRegularFile
      }
    }.pure[F]

  def processMarkerFile[F[_]](name: String,
                              path: Path,
                              dataFileMask: String,
                              markerFileMask: String,
                              transformerCommand: String)(implicit AF: Async[F]): F[Unit] =
    for {
      _ <- println(s"Got Marker File: '$name' in Path: '${path.toAbsolutePath.toString}'").pure[F]
      dataFileRegexp = (name.split("\\.").reverse.tail.reverse.mkString(".") + ".*").r
      predicate <- findPredicate(dataFileRegexp)
      dataPath = JFiles.find(path.getParent, 10, predicate).findFirst.get
      (dataName, dataSize) = (dataPath.getFileName.toString, JFiles.size(dataPath))
      (isExists, isFile) = (JFiles.exists(dataPath), JFiles.isRegularFile(dataPath))
      (isMarkerFile, isDataFile) = (dataName.matches(markerFileMask.r.regex), dataName.matches(dataFileMask.r.regex))
      _ <- AF.pure {
        println(s"Find Data File: '$dataName' with Size: ${dataSize / (1000.0 * 1000.0)}Mb " +
          s"in Path: '${dataPath.toAbsolutePath.toString}'")
        println(s"Path is ${if (isFile) "File" else "Directory"}")
        println(s"Path exists: $isExists")
        println(s"Path file name: '$name'")
        println(s"Path file name '$name' is " +
          s"${if (isMarkerFile) "Marker File" else if (isDataFile) "Data File" else "Other"}")
      }
      _ <- if (isExists && isDataFile && dataSize > 0) {
        processDataFile[F](dataName, dataPath, dataSize, transformerCommand)
      } else ().pure[F]
    } yield ()

  def processDataFile[F[_]](name: String,
                            path: Path,
                            size: Long,
                            transformerCommand: String)(implicit AF: Async[F]): F[Unit] =
    for {
      _ <- println(s"Got Data File: '$name' with Size: ${size / (1000.0 * 1000.0)}Mb " +
        s"in Path: '${path.toAbsolutePath.toString}'").pure[F]
      _ <- println(
        s"""#===============================================================#
           |Running external Spark-App process for Data File: '$name'"
           |#===============================================================#
           |""".stripMargin).pure[F]
      _ <- Executor[F].exec(
        transformerCommand,
        path.toAbsolutePath,
        path.getParent.resolve("out").toAbsolutePath
      )
    } yield ()
*/
}
