name := "sclera-parsing"

description := "Enables case-insensitive parsing and string escape sequences"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/util/parsing"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2" % "provided"
)
