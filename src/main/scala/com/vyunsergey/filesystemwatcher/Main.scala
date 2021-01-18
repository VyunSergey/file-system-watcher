package com.vyunsergey.filesystemwatcher

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
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
      _ <- IO {
        println(s"File mask for data file: '$fileMask'")
        println(s"Marker file mask for marker file: '$markerFileMask'")
        println(s"Watching path: '${path.toUri.getPath}'")
      }
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
            process(path, fileMask, markerFileMask)
          }
        case (Modified(path: Path, _: Int), size) =>
          IO {
            println(s"Got Modified path: '$path' with size: ${size / (1000.0 * 1000.0)}Mb")
            process(path, fileMask, markerFileMask)
          }
        case _ => println("Waiting for events: Created, Modified").pure[IO]
      }.compile.drain
    } yield {
      ExitCode.Success
    }
  }

  def process(path: Path,
              fileMask: String,
              markerFileMask: String): Unit = {
    val isExists = JFiles.exists(path)
    val isFile: Boolean = JFiles.isRegularFile(path)
    val fileName: String = path.getFileName.toString
    val fileRegexp: Regex = fileMask.r
    val markerFileRegexp: Regex = markerFileMask.r
    val isDataFile: Boolean = fileName.matches(fileRegexp.regex)
    val isMarkerFile: Boolean = fileName.matches(markerFileRegexp.regex)

    println(s"Path is ${if (isFile) "file" else "directory"}")
    println(s"Path exists: $isExists")
    println(s"Path file name: '$fileName'")
    println(s"Path file name '$fileName' is data file: $isDataFile")
    println(s"Path file name '$fileName' is marker file: $isMarkerFile")

    if (isExists && isMarkerFile) {
      println(s"Got marker file: '$fileName'")

      def findPredicate(regexp: Regex): BiPredicate[Path, BasicFileAttributes] =
        (t: Path, u: BasicFileAttributes) => {
          val matches: Boolean = t.getFileName.toString.matches(regexp.regex)

          println(s"  got path: '$t'")
          println(s"  path is file: ${u.isRegularFile}")
          println(s"  path matches regexp '${regexp.regex}': $matches")

          matches && u.isRegularFile
        }

      val dataFileRegexp: Regex =
        (fileName.split("\\.").reverse.tail.reverse.mkString(".") + ".*").r

      val dataFile: Path =
        JFiles.find(path.getParent, 10, findPredicate(dataFileRegexp)).findFirst.get

      val dataFileName: String = dataFile.getFileName.toString

      println(s"Find data file: '$dataFileName'")
      println(s"Going to process data file")
      processData(dataFile, JFiles.size(dataFile), fileMask)
    }
  }

  def processData(path: Path, size: Long, fileMask: String): Unit = {
    val isExists = JFiles.exists(path)
    val isFile: Boolean = JFiles.isRegularFile(path)
    val fileName: String = path.getFileName.toString
    val fileRegexp: Regex = fileMask.r
    val isDataFile: Boolean = fileName.matches(fileRegexp.regex)

    println(s"Path is ${if (isFile) "file" else "directory"}")
    println(s"Path exists: $isExists")
    println(s"Path file name: '$fileName'")
    println(s"Path file name '$fileName' is data file: $isDataFile")

    if (isExists && isDataFile && size > 0) {
      println(
        s"""#===============================================================#
           |Running external Spark-App process for data file: '$fileName'"
           |#===============================================================#
           |""".stripMargin)
    }
  }
}
