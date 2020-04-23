name := "sclera-jdbc"

description := "Sclera JDBC type 4 driver"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/interfaces/jdbc"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.1.1" % "test"
)

javaOptions in Test ++= Seq(
    s"-DSCLERA_ROOT=${java.nio.file.Files.createTempDirectory("scleratest")}"
)

fork in Test := true
