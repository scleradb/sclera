name := "sclera-visual"

description := "Visualization extension for Sclera"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/visual"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-math3" % "3.6.1",
    "com.typesafe.play" %% "play-json" % "2.8.1"
)
