name := "sclera-regexparser"

description := "Regular expression parser, generates a non-deterministic finite automata"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)
