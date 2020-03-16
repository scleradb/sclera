ThisBuild / organization := "com.scleradb"

ThisBuild / organizationName := "Sclera, Inc."

ThisBuild / organizationHomepage := Some(url("https://www.scleradb.com"))

ThisBuild / developers := List(
    Developer(
        id    = "prasanroy",
        name  = "Prasan Roy",
        email = "prasan@scleradb.com",
        url   = url("https://github.com/prasanroy")
    )
)

ThisBuild / pomIncludeRepository := { _ => false }

ThisBuild / publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true
