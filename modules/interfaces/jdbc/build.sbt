name := "sclera-jdbc"

description := "Sclera JDBC type 4 driver"

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

fork in Test := true
