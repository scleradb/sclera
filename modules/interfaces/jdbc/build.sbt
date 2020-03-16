name := "sclera-jdbc"

description := "Sclera JDBC type 4 driver"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

fork in Test := true
