name := "TheSeersAccuracy"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.0"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"