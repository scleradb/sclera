name := "sclera-core"

description := "Sclera core, classes for Sclera extension development in Scala"

apiURL := Some(url("https://scleradb.github.io/sclera-core-sdk/"))

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    "com.h2database" % "h2" % "1.4.200",
    "com.zaxxer" % "HikariCP" % "3.4.1",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "provided",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

publishArtifact in (Compile, packageDoc) := true

fork in Test := true
