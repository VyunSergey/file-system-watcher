package com.vyunsergey.filesystemwatcher.common.arguments

import org.rogach.scallop.{ScallopConf, ScallopOption}

import java.nio.file.{Path, Files => JFiles}
import scala.util.Try

class ArgumentsOps(args: Seq[String]) extends ScallopConf(args) {
  def validatePath(path: Path): Boolean = {
    Try(JFiles.exists(path)).getOrElse(false)
  }

  val path: ScallopOption[Path] = opt[Path](name = "path", validate = validatePath)
  val configPath: ScallopOption[Path] = opt[Path](name = "config-path", validate = validatePath)
  verify()
}
