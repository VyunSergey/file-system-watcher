lazy val scalaVersion2_13   = "2.13.4"
scalaVersion := scalaVersion2_13
val catsVersion             = "2.2.0"
val catsEffectVersion       = "2.3.0-M1"
val fs2Version              = "3.0.0-M1"
val fs2IoVersion            = "3.0.0-M1"
val tofuVersion             = "0.8.0"
val derevoVersion           = "0.11.5"
val PureConfigVersion       = "0.12.0"
val ScallopVersion          = "4.0.0"
val KindProjectorVersion    = "0.11.2"
val BetterMonadicForVersion = "0.3.1"
val ScalaTestVersion        = "3.2.2"
val ScalaCheckVersion       = "1.14.1"
val ScalaTestPlusVersion    = "3.2.2.0"

lazy val organizationSettings = Seq(
  organization := "com.vyunsergey",
  name := "file-system-watcher",
  homepage := Some(url("https://github.com/VyunSergey")),
  licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html"))),
  idePackagePrefix := Some("com.vyunsergey.filesystemwatcher")
)

lazy val commonLibraryDependencies = Seq(
  // Cats
  "org.typelevel"              %% "cats-core"          % catsVersion,
  // Cats-effect
  "org.typelevel"              %% "cats-effect"        % catsEffectVersion,
  // FS2-core
  "co.fs2"                     %% "fs2-core"           % fs2Version,
  // FS2-io
  "co.fs2"                     %% "fs2-io"             % fs2IoVersion,
  // PureConfig
  "com.github.pureconfig"      %% "pureconfig"         % PureConfigVersion,
  // Scallop
  "org.rogach"                 %% "scallop"            % ScallopVersion,
  // Tofu
  "ru.tinkoff"                 %% "tofu"               % tofuVersion,
  // Derevo
  "org.manatki"                %% "derevo-cats"        % derevoVersion,
  // KIND Projector
  "org.typelevel"              %  s"kind-projector_$scalaVersion2_13" % KindProjectorVersion,
  // ScalaTest
  "org.scalatest"              %% "scalatest"          % ScalaTestVersion % Test,
  // ScalaCheck
  "org.scalacheck"             %% "scalacheck"         % ScalaCheckVersion % Test,
  // ScalaTestPlus
  "org.scalatestplus"          %% "scalacheck-1-14"    % ScalaTestPlusVersion % Test
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
  libraryDependencies ++= commonLibraryDependencies,
  scalacOptions ++= scalaCompilerOptions,
  addCompilerPlugin("com.olegpy"   %% "better-monadic-for" % BetterMonadicForVersion)
)
