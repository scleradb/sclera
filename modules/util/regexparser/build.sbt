name := "sclera-regexparser"

description := "Regular expression parser, generates a non-deterministic finite automata"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/util/regexparser"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided",
    "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)
