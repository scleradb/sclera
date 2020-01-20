name := "sclera-config"

description := "Configuration manager for Sclera"

libraryDependencies ++= Seq(
    "com.typesafe" % "config" % "1.4.0",
    "ch.qos.logback" % "logback-classic" % "1.2.3" exclude("org.slf4j", "slf4j-log4j12")
)

publishArtifact in (Compile, packageDoc) := true
