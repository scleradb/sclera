name := "sclera-encrypt"

description := "Data encryptor and decryptor"

libraryDependencies ++= Seq(
    "commons-codec" % "commons-codec" % "1.13"
)

publishArtifact in (Compile, packageDoc) := true
