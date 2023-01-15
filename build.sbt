 lazy val root = (project in file("."))
.settings(
    name           := "BigData",
    organization    := "hkanalytics",
    scalaVersion    := "2.13.8",
    version         := "0.1.0-SNAPSHOT",
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1",
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "compile",
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1" % "compile",
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.2.1",
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.1" % "compile",
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
)

scalafmtOnCompile := true