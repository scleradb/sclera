name := "sclera-parsing"

description := "Enables case-insensitive parsing and string escape sequences"

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided"
)

publishArtifact in (Compile, packageDoc) := true
