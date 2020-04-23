name := "sclera-config"

description := "Configuration manager for Sclera"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/config"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "com.typesafe" % "config" % "1.4.0",
    "ch.qos.logback" % "logback-classic" % "1.2.3" exclude("org.slf4j", "slf4j-log4j12")
)
