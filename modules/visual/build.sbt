name := "sclera-visual"

description := "Visualization extension for Sclera"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-math3" % "3.6.1",
    "com.typesafe.play" %% "play-json" % "2.8.1"
)
