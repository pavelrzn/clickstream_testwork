name := "clickstream"

version := "0.2"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  , "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)