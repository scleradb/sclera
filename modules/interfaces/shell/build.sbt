name := "sclera-shell"

description := "Sclera interactive shell"

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "provided",
    "org.jline" % "jline-terminal-jansi" % "3.13.1",
    "org.jline" % "jline-reader" % "3.13.1",
    "org.jline" % "jline-builtins" % "3.13.1",
    "org.fusesource.jansi" % "jansi" % "1.18",
    "org.apache.commons" % "commons-csv" % "1.7"
)

fork in run := true

val mkscript = taskKey[File]("Create executable script")

mkscript := {
    val base = baseDirectory.value
    val cp = (fullClasspath in Test).value
    val main = (mainClass in Runtime).value
    val (template, scriptName) =
        if( System.getProperty("os.name").startsWith("Windows") ) (
            """|@ECHO OFF
               |java -Xmx512m -classpath "%%CLASSPATH%%:%s" %s %%*
               |""".stripMargin,
            "sclera.cmd"
        ) else (
            """|#!/bin/sh
               |java -Xmx512m -classpath "$CLASSPATH:%s" %s $@
               |""".stripMargin,
            "sclera"
        )
    val mainStr = main getOrElse sys.error("No main class specified")
    val contents = template.format(cp.files.absString, mainStr)
    val out = base / "bin" / scriptName
    IO.write(out, contents)
    out.setExecutable(true)
    out
}
