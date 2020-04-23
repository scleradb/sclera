name := "sclera-extensions-java-sdk"

description := "Wrapper classes to enable Sclera extension development in Java"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/sdkjava"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "provided"
)
