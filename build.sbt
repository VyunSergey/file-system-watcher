lazy val scalaVersion2_13   = "2.13.4"
scalaVersion := scalaVersion2_13
val catsVersion             = "2.6.1"
val catsEffectVersion       = "2.5.3"
val circeVersion            = "0.14.1"
val fs2Version              = "2.5.9"
val fs2IoVersion            = "2.5.9"
val tofuVersion             = "0.9.2"
val PureConfigVersion       = "0.16.0"
val ScallopVersion          = "4.0.3"
val KindProjectorVersion    = "0.11.2"
val zip4jVersion            = "2.9.0"
val BetterMonadicForVersion = "0.3.1"
val ScalaTestVersion        = "3.2.9"
val ScalaCheckVersion       = "1.15.4"
val ScalaTestPlusVersion    = "3.2.9.0"

lazy val organizationSettings = Seq(
  organization := "com.vyunsergey",
  name := "file-system-watcher",
  homepage := Some(url("https://github.com/VyunSergey")),
  licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")))
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"${name.value}-${scalaVersion.value}_${version.value}.jar",
  assembly / mainClass := Some("com.vyunsergey.filesystemwatcher.Main"),
  assembly / test := {},
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("org.apache.http.**" -> "shaded.org.apache.http.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ :: Nil => MergeStrategy.concat
      case name :: Nil =>
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF")) MergeStrategy.discard
        else MergeStrategy.first
      case _ => MergeStrategy.first
    }
    case _ => MergeStrategy.first
  }
)

lazy val commonLibraryDependencies = Seq(
  // Cats
  "org.typelevel"              %% "cats-core"               % catsVersion,
  // Cats-effect
  "org.typelevel"              %% "cats-effect"             % catsEffectVersion,
  // Circe-core
  "io.circe"                   %% "circe-core"              % circeVersion,
  // Circe-generic
  "io.circe"                   %% "circe-generic"           % circeVersion,
  // Circe-parser
  "io.circe"                   %% "circe-parser"            % circeVersion,
  // FS2-core
  "co.fs2"                     %% "fs2-core"                % fs2Version,
  // FS2-io
  "co.fs2"                     %% "fs2-io"                  % fs2IoVersion,
  // PureConfig
  "com.github.pureconfig"      %% "pureconfig"              % PureConfigVersion,
  // Scallop
  "org.rogach"                 %% "scallop"                 % ScallopVersion,
  // Tofu
  "ru.tinkoff"                 %% "tofu"                    % tofuVersion,
  // Tofu Logging
  "ru.tinkoff"                 %% "tofu-logging"            % tofuVersion,
  // Tofu Logging Layout
  "ru.tinkoff"                 %% "tofu-logging-layout"     % tofuVersion,
  // Tofu Logging Derivation
  "ru.tinkoff"                 %% "tofu-logging-derivation" % tofuVersion,
  // KIND Projector
  "org.typelevel"              %  s"kind-projector_$scalaVersion2_13" % KindProjectorVersion,
  // Zip4j
  "net.lingala.zip4j"          % "zip4j"                    % zip4jVersion,
  // ScalaTest
  "org.scalatest"              %% "scalatest"               % ScalaTestVersion % Test,
  // ScalaCheck
  "org.scalacheck"             %% "scalacheck"              % ScalaCheckVersion % Test,
  // ScalaTestPlus
  "org.scalatestplus"          %% "scalacheck-1-15"         % ScalaTestPlusVersion % Test
)

lazy val scalaCompilerOptions = Seq(
  "-deprecation",                  // Emit warning and location for usages of deprecated APIs
  "-unchecked",                    // Enable additional warnings where generated code depends on assumptions
  "-feature",                      // Emit warning and location for usages of features that should be imported explicitly
  "-encoding", "UTF-8",            // Specify character encoding used by source files
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-language:existentials",        // Existential types (besides wildcard types) can be written and inferred
  "-language:higherKinds",         // Allow higher-kinded types
  "-language:postfixOps",          // Allows operator syntax in postfix position (deprecated since Scala 2.10)
  "-Xfatal-warnings"               // Fail the compilation if there are any warnings
)

lazy val root = (project in file(".")).settings(
  organizationSettings,
  assemblySettings,
  libraryDependencies ++= commonLibraryDependencies,
  scalacOptions ++= scalaCompilerOptions,
  addCompilerPlugin("com.olegpy"   %% "better-monadic-for" % BetterMonadicForVersion)
)
