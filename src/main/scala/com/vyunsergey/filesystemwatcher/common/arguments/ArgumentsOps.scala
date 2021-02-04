package com.vyunsergey.filesystemwatcher.common.arguments

import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.nio.file.{Files, Path}

class ArgumentsOps(args: Seq[String]) extends ScallopConf(args) {
  val path: ScallopOption[Path] = opt[Path](name = "path", validate = Files.exists(_))
  val configPath: ScallopOption[Path] = opt[Path](name = "config-path", validate = Files.exists(_))
  verify()
}
