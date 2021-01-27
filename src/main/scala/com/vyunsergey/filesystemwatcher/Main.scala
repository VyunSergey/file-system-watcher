package com.vyunsergey.filesystemwatcher

import cats.effect.{Async, ExitCode, IO, IOApp}
import cats.implicits._
import com.vyunsergey.filesystemwatcher.common.TransformerExecutor
import com.vyunsergey.filesystemwatcher.common.arguments.ArgumentsReader
import com.vyunsergey.filesystemwatcher.common.configuration.ConfigReader
import fs2.io.Watcher.Event._

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Path, Files => JFiles}
import java.util.function.BiPredicate
import scala.util.matching.Regex

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    for {
      arguments <- ArgumentsReader[IO](args)
      _ <- println(s"Arguments: ${arguments.args.mkString("['", "', '", "']")}").pure[IO]
      configPath <- arguments.configPath.toOption.map(ConfigReader[IO].convertPath)
        .getOrElse(ConfigReader.configPathDefault.pure[IO])
      config <- ConfigReader[IO].read(configPath)
      _ <- println(s"Configuration: $config").pure[IO]
      path <- arguments.path.toOption.map(ConfigReader[IO].convertPath).getOrElse(config.path.pure[IO])
      fileMask <- arguments.fileMask.toOption.getOrElse(config.fileMask).pure[IO]
      markerFileMask <- arguments.markerFileMask.toOption.getOrElse(config.markerFileMask).pure[IO]
      transformerKeyValMode <- arguments.transformerKeyValMode.toOption.getOrElse(config.transformerKeyValMode).pure[IO]
      transformerKeyValPath <- arguments.transformerKeyValPath.toOption.getOrElse(config.transformerKeyValPath).pure[IO]
      transformerKeyValOptions <- arguments.transformerKeyValOptions.toOption.getOrElse(config.transformerKeyValOptions).pure[IO]
      transformerKeyValExecCommand <- arguments.transformerKeyValExecCommand.toOption.getOrElse(config.transformerKeyValExecCommand).pure[IO]
      _ <- IO {
        println(s"Watching path: '${path.toAbsolutePath.toString}'")
        println(s"File mask for data file: '$fileMask'")
        println(s"Marker file mask for marker file: '$markerFileMask'")
        println(s"Transformer Key-Val mode: '$transformerKeyValMode'")
        println(s"Transformer Key-Val path: '$transformerKeyValPath'")
        println(s"Transformer Key-Val options: '$transformerKeyValOptions'")
        println(s"Transformer Key-Val exec command: '$transformerKeyValExecCommand'")
      }
      transformerCommand <- TransformerExecutor[IO].createExecutorCommand(
        transformerKeyValMode,
        transformerKeyValPath,
        transformerKeyValOptions,
        transformerKeyValExecCommand
      )
      watcher = FileSystemWatcher[IO]
      stream <- watcher.watchWithFileSize(path)
      _ <- stream
        .debug { case (event, size) =>
          s"""#===============================================================#
             |Event: $event with file size: ${size / (1000.0 * 1000.0)}Mb
             |#===============================================================#
             |""".stripMargin
        }.evalMap {
        case (Created(path: Path, _: Int), size) =>
          IO {
            println(s"Got Created path: '$path' with size: ${size / (1000.0 * 1000.0)}Mb")
          } *> process[IO](path.getFileName.toString, path, fileMask, markerFileMask, transformerCommand)
//        case (Modified(path: Path, _: Int), size) =>
//          IO {
//            println(s"Got Modified path: '$path' with size: ${size / (1000.0 * 1000.0)}Mb")
//          } *> process[IO](path.getFileName.toString, path, fileMask, markerFileMask, transformerCommand)
        case _ => println("Waiting for events: Created, Modified").pure[IO]
      }.compile.drain
    } yield {
      ExitCode.Success
    }
  }

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
      _ <- TransformerExecutor[F].exec(
        transformerCommand,
        path.toAbsolutePath,
        path.getParent.resolve("out").toAbsolutePath
      )
    } yield ()

}
