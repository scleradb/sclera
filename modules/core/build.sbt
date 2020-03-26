name := "sclera-core"

description := "Sclera core, classes for Sclera extension development in Scala"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    "com.h2database" % "h2" % "1.4.200",
    "com.zaxxer" % "HikariCP" % "3.4.1",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "provided",
    "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

javaOptions in Test ++= Seq(
    s"-DSCLERA_ROOT=${java.nio.file.Files.createTempDirectory("scleratest")}"
)

fork in Test := true
