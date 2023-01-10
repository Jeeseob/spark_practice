name:= "scala_example"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.10"

val sparkVersion = "3.3.1" //현재 설치 버전

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion