package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path}
import java.util.function.BiPredicate
import scala.annotation.tailrec
import scala.util.Try
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
           operation: Path => String,
           fileFilter: BasicFileAttributes => Boolean,
           matchRegexp: Option[Regex] = None,
           mismatchRegexp: Option[Regex] = None,
           maxDepth: Int = 10): F[Option[Path]] =
    for {
      predicate <- debug"Creating BiPredicate for regex: (match = '${matchRegexp.map(_.regex)}', mismatch = '${mismatchRegexp.map(_.regex)}')" as {
        new BiPredicate[Path, BasicFileAttributes] {
          def test(t: Path, u: BasicFileAttributes): Boolean = {
            val matches: Boolean = matchRegexp.forall(reg => operation(t).matches(reg.regex))
            val mismatches: Boolean = mismatchRegexp.exists(reg => operation(t).matches(reg.regex))
            matches && !mismatches && fileFilter(u)
          }
        }
      }
      resultPathJOp <- info"Finding first subPath in Path: '${path.toAbsolutePath.toString}' with regex: (match = '${matchRegexp.map(_.regex)}', mismatch = '${mismatchRegexp.map(_.regex)}')" as {
        Files.find(path, maxDepth, predicate).findFirst
      }
      resultPathOp <- debug"Converting Java Optional to Scala Option" as {
        resultPathJOp match {
          case null => None
          case _ => if (resultPathJOp.isPresent) Some(resultPathJOp.get) else None
        }
      }
    } yield resultPathOp

  def findParent(path: Path,
                 operation: Path => String,
                 matchRegexp: Option[Regex] = None,
                 mismatchRegexp: Option[Regex] = None,
                 maxDepth: Int = 10): F[Option[Path]] = {
    @tailrec
    def inner(path: Path,
              operation: Path => String,
              matchRegexp: Option[Regex],
              mismatchRegexp: Option[Regex],
              maxDepth: Int,
              currentDepth: Int = 0): Option[Path] = {
      if (currentDepth > maxDepth || path == null) None
      else {
        val matches: Boolean = Try(matchRegexp.forall(reg => operation(path).matches(reg.regex))).getOrElse(false)
        val mismatches: Boolean = Try(mismatchRegexp.exists(reg => operation(path).matches(reg.regex))).getOrElse(true)
        if (matches && !mismatches) Some(path)
        else inner(path.getParent, operation, matchRegexp, mismatchRegexp, maxDepth, currentDepth + 1)
      }
    }

    for {
      resultPathOp <- info"Finding first Parent Path in Path: '${path.toAbsolutePath.toString}' with regex: (match = '${matchRegexp.map(_.regex)}', mismatch = '${mismatchRegexp.map(_.regex)}')" as {
        inner(path, operation, matchRegexp, mismatchRegexp, maxDepth)
      }
    } yield resultPathOp
  }
}

object FileProcessor {
  def apply[F[_]: Monad](logs: Logs[F, F]): Resource[F, FileProcessor[F]] =
    Resource.liftF(logs.forService[FileProcessor[F]].map(implicit l => new FileProcessor[F]))
}
