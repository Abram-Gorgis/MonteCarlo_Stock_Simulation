name := "homework3_4"

version := "1.0"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies +="com.amazonaws" % "aws-java-sdk" % "1.11.460"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies +="org.slf4j" % "slf4j-api" % "1.7.28"
libraryDependencies +="com.typesafe" % "config" % "1.4.0"
libraryDependencies +=  "junit" % "junit" % "4.12"
fork:= true

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}