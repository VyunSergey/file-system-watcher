package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path}
import java.util.function.BiPredicate
import scala.util.matching.Regex

class FileProcessor[F[_]: Monad: Logging] {
  def process(path: Path)(operation: Path => F[Unit]): F[Unit] =
    for {
      isExist <- Files.exists(path).pure[F]
      isFile <- Files.isRegularFile(path).pure[F]
      entity <- (if (isFile) s"File" else "Directory").pure[F]
      size <- Files.size(path).pure[F]
      p <- info"Received $entity: '${path.toAbsolutePath.toString}' with Size ${size / (1000.0 * 1000.0)}Mb" as path
      _ <- if (isExist) {
        if (isFile) operation(p)
        else info"Waiting Files, Skipping $entity: '${path.toAbsolutePath.toString}'"
      } else {
        warn"$entity: '${path.toAbsolutePath.toString}' does not exists, Skipping"
      }
    } yield ()

  def find(path: Path,
           matchRegexp: Option[Regex] = None,
           mismatchRegexp: Option[Regex] = None,
           maxDepth: Int = 10): F[Option[Path]] =
    for {
      predicate <- debug"Creating BiPredicate for regex: (match = '${matchRegexp.map(_.regex)}', mismatch = '${mismatchRegexp.map(_.regex)}')" as {
        new BiPredicate[Path, BasicFileAttributes] {
          def test(t: Path, u: BasicFileAttributes): Boolean = {
            val matches: Boolean = matchRegexp.forall(reg => t.getFileName.toString.matches(reg.regex))
            val mismatch: Boolean = mismatchRegexp.exists(reg => t.getFileName.toString.matches(reg.regex))
            matches && !mismatch && u.isRegularFile
          }
        }
      }
      resultPathJOp <- info"Finding first subPath in Path: '${path.toAbsolutePath.toString}' with regex: (match = '${matchRegexp.map(_.regex)}', mismatch = '${mismatchRegexp.map(_.regex)}')" as {
        Files.find(path, maxDepth, predicate).findFirst
      }
      resultPathOp <- debug"Converting Java Optional to Scala Option" as {
        resultPathJOp match {
          case null => null
          case _ => if (resultPathJOp.isPresent) Some(resultPathJOp.get) else None
        }
      }
    } yield resultPathOp
}

object FileProcessor {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, FileProcessor[F]] =
    Resource.liftF(logs.forService[FileProcessor[F]].map(implicit l => new FileProcessor[F]))
}
