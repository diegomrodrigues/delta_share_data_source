name := "DeltaShareChangeDataSource"

version := "0.1"

scalaVersion := "2.12.15" // Ensure this matches the Scala version of your Databricks cluster

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",       // Match your Spark version
  "io.delta" %% "delta-core" % "2.1.0",              // Match your Delta Lake version
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0" % Provided
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.3"
