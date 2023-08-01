ThisBuild / scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"

resolvers += "Nexus local" at "https://nexus-repo.dmp.vimpelcom.ru/repository/sbt_releases_/"
resolvers += "Nexus proxy" at "https://nexus-repo.dmp.vimpelcom.ru/repository/maven_all_/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.3.3")



