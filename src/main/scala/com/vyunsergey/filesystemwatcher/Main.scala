package com.vyunsergey.filesystemwatcher

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.vyunsergey.filesystemwatcher.common.arguments.ArgumentsReader
import com.vyunsergey.filesystemwatcher.common.configuration.ConfigReader

object Main extends IOApp {
  def run(args: List[String]): IO[cats.effect.ExitCode] = {
    for {
      arguments <- ArgumentsReader[IO](args)
      _ <- println(s"Arguments: ${arguments.args.mkString("['", "', '", "']")}").pure[IO]
      configPath <- arguments.configPath.toOption.map(ConfigReader[IO].convertPath)
        .getOrElse(ConfigReader.configPathDefault.pure[IO])
      config <- ConfigReader[IO].read(configPath)
      _ <- println(s"Configuration: $config").pure[IO]
      path <- arguments.path.toOption.map(ConfigReader[IO].convertPath).getOrElse(config.path.pure[IO])
      _ <- println(s"Watching path: ${path.toUri.getPath}").pure[IO]
      stream <- FileSystemWatcher[IO].watchWithFileSize(path)
      _ <- stream.debug { case (event, size) =>
        s"Event: $event with file size: ${size / (1000.0 * 1000.0)}Mb"
      }.compile.drain
    } yield {
      ExitCode.Success
    }
  }
}
