name := "sclera-regexparser"

description := "Regular expression parser, generates a non-deterministic finite automata"

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided"
)
