ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "BIG_DATA"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "3.0.0"
libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "2.1.0",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "2.1.0"
)

libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.4"

libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.8.4"


libraryDependencies += "org.scalanlp" %% "breeze" % "1.2"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.2"



libraryDependencies  ++= Seq(
  "org.scalanlp" %% "breeze" % "2.1.0",
  "org.scalanlp" %% "breeze-viz"%"2.1.0"
)


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib"%"3.5.0"
libraryDependencies += "mysql" % "mysql-connector-java"%"8.0.33"
