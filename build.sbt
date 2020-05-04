name := "sclera"

description := "Sclera Analytics Visualization Platform"

homepage := Some(url(s"https://github.com/scleradb/${name.value}"))

ThisBuild / scmInfo := Some(
    ScmInfo(
        url(s"https://github.com/scleradb/${name.value}"),
        s"scm:git@github.com:scleradb/${name.value}.git"
    )
)

ThisBuild / version := "4.0.1-SNAPSHOT"

ThisBuild / startYear := Some(2012)

ThisBuild / licenses := Seq("Apache License version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

ThisBuild / scalaVersion := "2.13.1"

ThisBuild / scalacOptions ++= Seq(
    "-Werror", "-feature", "-deprecation", "-unchecked",
    "-Dscalac.patmat.analysisBudget=2048",
    "-Ypatmat-exhaust-depth", "40"
)

ThisBuild / exportJars := true

ThisBuild / autoAPIMappings := true

ThisBuild / resolvers +=
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/"

publish / skip := true

lazy val sclera = Project(
    id = "sclera",
    base = file(".")
) aggregate (
    config,
    service,
    tools,
    encrypt,
    automata,
    parsing,
    regexparser,
    core,
    visual,
    jdbc,
    shell,
    display,
    sdkjava
)

lazy val core = Project(
    id = "sclera-core",
    base = file("modules/core")
) dependsOn (
    config % "provided", parsing, tools, regexparser, automata, encrypt, service
)

lazy val config = Project(
    id = "sclera-config",
    base = file("modules/config")
)

lazy val service = Project(
    id = "sclera-service",
    base = file("modules/service")
) dependsOn (
    config % "provided"
)

lazy val tools = Project(
    id = "sclera-tools",
    base = file("modules/util/tools")
)

lazy val encrypt = Project(
    id = "sclera-encrypt",
    base = file("modules/util/encrypt")
)

lazy val automata = Project(
    id = "sclera-automata",
    base = file("modules/util/automata")
) dependsOn (tools % "provided")

lazy val parsing = Project(
    id = "sclera-parsing",
    base = file("modules/util/parsing")
)

lazy val regexparser = Project(
    id = "sclera-regexparser",
    base = file("modules/util/regexparser")
) dependsOn (
    automata % "provided", parsing % "provided"
)

lazy val visual = Project(
    id = "sclera-visual",
    base = file("modules/visual")
) dependsOn (
    core % "provided"
)

lazy val jdbc = Project(
    id = "sclera-jdbc",
    base = file("modules/interfaces/jdbc")
) dependsOn (
    core % "provided", config % "test"
)

lazy val shell = Project(
    id = "sclera-shell",
    base = file("modules/interfaces/shell")
) dependsOn (
    core % "provided", config % "provided", visual, display
)

lazy val display = Project(
    id = "sclera-display",
    base = file("modules/interfaces/display")
) dependsOn (
    config % "provided", service % "provided"
)

lazy val sdkjava = Project(
    id = "sclera-extensions-java-sdk",
    base = file("sdkjava")
) dependsOn (
    core % "provided"
)
