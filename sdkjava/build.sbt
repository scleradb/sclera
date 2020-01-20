name := "sclera-extensions-java-sdk"

description := "Wrapper classes to enable Sclera extension development in Java"

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "provided"
)

publishArtifact in (Compile, packageDoc) := true
