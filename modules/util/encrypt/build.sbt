name := "sclera-encrypt"

description := "Data encryptor and decryptor"

homepage := Some(url("https://github.com/scleradb/sclera/tree/master/modules/util/encrypt"))

scmInfo := Some(
    ScmInfo(
        url("https://github.com/scleradb/sclera"),
        "scm:git@github.com:scleradb/sclera.git"
    )
)

libraryDependencies ++= Seq(
    "commons-codec" % "commons-codec" % "1.13"
)
