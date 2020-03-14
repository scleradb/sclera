name := "sclera-regexparser"

description := "Regular expression parser, generates a non-deterministic finite automata"

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided",
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)
