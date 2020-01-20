name := "sclera-visual"

description := "Visualization extension for Sclera"

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-math3" % "3.6.1",
    "com.typesafe.play" %% "play-json" % "2.8.1"
)

publishArtifact in (Compile, packageDoc) := true
