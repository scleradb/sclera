name := "sclera-extensions-java-sdk"

description := "Wrapper classes to enable Sclera extension development in Java"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "provided"
)
