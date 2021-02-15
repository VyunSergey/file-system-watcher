package com.vyunsergey.filesystemwatcher.processor.file

import cats.Monad
import cats.effect.Resource
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod
import tofu.logging._
import tofu.syntax.logging._
import tofu.syntax.monadic._

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.function.BiPredicate
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
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

  def readFile(path: Path, sep: String = System.lineSeparator): F[Option[String]] = {
    Try {
      val br = new BufferedReader(new InputStreamReader(Files.newInputStream(path)))
      try {
        val sb = new StringBuilder
        var line = br.readLine

        while (line != null) {
          sb.append(line)
          sb.append(sep)
          line = br.readLine
        }
        sb.toString
      } finally {
        br.close()
      }
    }.toOption.pure[F]
  }

  def moveFile(srcPath: Path, tgtPath: Path): F[Unit] = {
    for {
      _ <- debug"Moving data from Source Path: '${srcPath.toAbsolutePath.toString}' to Target Path: '${tgtPath.toAbsolutePath.toString}' with copy option: ${StandardCopyOption.REPLACE_EXISTING.toString}" as {
        Files.move(srcPath, tgtPath, StandardCopyOption.REPLACE_EXISTING)
      }
      _ <- debug"Finish moving data from Source Path: '${srcPath.toAbsolutePath.toString}' to Target Path: '${tgtPath.toAbsolutePath.toString}'"
    } yield ()
  }

  def renameFile(srcPath: Path, name: String): F[Unit] = {
    for {
      _ <- debug"Renaming file '${srcPath.getFileName.toString}' from Path: '${srcPath.toAbsolutePath.toString}' to file '$name'" as {
        moveFile(srcPath, srcPath.getParent.resolve(name))
      }
      _ <- debug"Finish Renaming file '${srcPath.getFileName.toString}' from Path: '${srcPath.toAbsolutePath.toString}' to file '$name'"
    } yield ()
  }

  def deleteFile(path: Path): F[Unit] = {
    for {
      _ <- debug"Deleting file '${path.getFileName.toString}' from Path: '${path.toAbsolutePath.toString}'" as {
        Files.deleteIfExists(path)
      }
      _ <- debug"Finish deleting file '${path.getFileName.toString}' from Path: '${path.toAbsolutePath.toString}'"
    } yield ()
  }

  def zipFiles(paths: List[Path], zipPath: Path): F[Unit] = {
    for {
      _ <- debug"Zipping files: ${paths.map(_.toFile.getName)} to archive '${zipPath.getFileName.toString}'" as {
        new ZipFile(zipPath.toFile).addFiles(paths.map(_.toFile).asJava)
      }
      _ <- debug"Finish zipping files: ${paths.map(_.toFile.getName)} to archive '${zipPath.getFileName.toString}'"
    } yield ()
  }

  def zipFilesDivided(paths: List[Path], zipPath: Path,
                      size: Long = 100 * 1024 * 1024): F[Unit] = {
    for {
      zipParameters <- debug"Creating zip parameters" as {
        new ZipParameters()
      }
      _ <- debug"Zipping files: ${paths.map(_.toFile.getName)} to archive '${zipPath.getFileName.toString}' divided by size ${size / 1024 / 1024}Mb" as {
        new ZipFile(zipPath.toFile).createSplitZipFile(paths.map(_.toFile).asJava, zipParameters, true, size)
      }
      _ <- debug"Finish zipping files: ${paths.map(_.toFile.getName)} to archive '${zipPath.getFileName.toString}' divided by size ${size / 1024 / 1024}Mb"
    } yield ()
  }

  def zipFilesProtected(paths: List[Path], zipPath: Path, password: String): F[Unit] = {
    for {
      _ <- debug"Zipping files: ${paths.map(_.toFile.getName)} to protected archive '${zipPath.getFileName.toString}' with password: '$password'" as {
        new ZipFile(zipPath.toFile, password.toCharArray).addFiles(paths.map(_.toFile).asJava)
      }
      _ <- debug"Finish zipping files: ${paths.map(_.toFile.getName)} to archive '${zipPath.getFileName.toString}'"
    } yield ()
  }

  def zipFilesProtectedDivided(paths: List[Path], zipPath: Path, password: String,
                               size: Long = 100 * 1024 * 1024): F[Unit] = {
    for {
      zipParameters <- debug"Creating zip parameters" as {
        val zipParams = new ZipParameters()
        zipParams.setEncryptFiles(true)
        zipParams.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD)
        zipParams
      }
      _ <- debug"Zipping files: ${paths.map(_.toFile.getName)} to protected archive '${zipPath.getFileName.toString}' with password: '$password' divided by size ${size / 1024 / 1024}Mb" as {
        new ZipFile(zipPath.toFile, password.toCharArray).createSplitZipFile(paths.map(_.toFile).asJava, zipParameters, true, size)
      }
      _ <- debug"Finish zipping files: ${paths.map(_.toFile.getName)} to protected archive '${zipPath.getFileName.toString}' with password: '$password' divided by size ${size / 1024 / 1024}Mb"
    } yield ()
  }

  def unzipFiles(zipPath: Path, path: Path): F[Unit] = {
    for {
      _ <- debug"Unzipping archive '${zipPath.getFileName.toString}' to Path: '${path.toAbsolutePath.toString}'" as {
        new ZipFile(zipPath.toFile).extractAll(path.toAbsolutePath.toString)
      }
      _ <- debug"Finish unzipping archive '${zipPath.getFileName.toString}' to Path: '${path.toAbsolutePath.toString}'"
    } yield ()
  }

  def unzipFilesProtected(zipPath: Path, path: Path, password: String): F[Unit] = {
    for {
      _ <- debug"Unzipping protected archive '${zipPath.getFileName.toString}' with password: '$password' to Path: '${path.toAbsolutePath.toString}'" as {
        new ZipFile(zipPath.toFile, password.toCharArray).extractAll(path.toAbsolutePath.toString)
      }
      _ <- debug"Finish unzipping protected archive '${zipPath.getFileName.toString}' with password: '$password' to Path: '${path.toAbsolutePath.toString}'"
    } yield ()
  }

  def getSubPaths(path: Path, maxDepth: Int = 10): F[List[Path]] = {
    for {
      subPaths <- debug"Getting All sub paths of Path: ${path.toAbsolutePath.toString}" as {
        Files.walk(path, maxDepth).filter(!_.equals(path)).iterator().asScala.toList
      }
      _ <- debug"Finish got sub paths: '${subPaths.map(_.toAbsolutePath.toString)}'"
    } yield subPaths
  }

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
