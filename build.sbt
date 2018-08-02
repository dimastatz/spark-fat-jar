import sbtassembly.AssemblyPlugin.autoImport.PathList

name := "spark-normalizer"
organization := "com.dimastatz"

scalaVersion := "2.11.8"

// Resolvers for Maven2 repositories
resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io",
  "MVN Repository" at "http://mvnrepository.com/",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Typesafe Repository" at "http://dl.bintray.com/typesafe/maven-releases/",
  "The New Motion Public Repo" at "http://nexus.thenewmotion.com/content/groups/public/"
  //"Local Nexus Repository" at "http://rg-nexus:8080/nexus/content/repositories/nexus-pipeline/"
)


libraryDependencies ++= Seq(
  "net.codingwell" %% "scala-guice" % "4.1.0",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "com.librato.metrics" % "metrics-librato" % "5.0.0",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.0"

).map(_.exclude("org.slf4j", "*"))

//insert one without exclusion
libraryDependencies ++= Seq(
  "org.slf4j" % "jul-to-slf4j" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)

mainClass in Compile := Some("com.dimastatz.sit.Boot")
mainClass in assembly := Some("com.dimastatz.sit.Boot")
mainClass in packageBin := Some("com.dimastatz.sit.Boot")

Revolver.settings: Seq[sbt.Def.Setting[_]]
enablePlugins(JavaServerAppPackaging, RpmPlugin, RpmDeployPlugin)
rpmVendor := "dimastatz"
rpmLicense := Some("dimastatz")
packageName in Rpm := s"${name.value}"
// we specify the name for our fat jar
assemblyJarName := s"${name.value}_${version.value}.jar"
// the bash scripts classpath only needs the fat jar
scriptClasspath := Seq(assemblyJarName.value)

packageArchitecture in Rpm := "noarch"
packageSummary in Rpm := "ct normalizer service"
packageDescription in Linux := "ct normalizer service"
daemonUser in Linux := "ec2-user"
maintainer in Linux := "Logingest Team"

defaultLinuxInstallLocation := sys.props.getOrElse("dimastatzfolder", default = "/opt/dimastatz")
rpmPrefix := Some(defaultLinuxInstallLocation.value)
linuxPackageSymlinks := Seq.empty
defaultLinuxLogsLocation := "/var/log"


// removes all jar mappings in universal and appends the fat jar
mappings in Universal := {
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, fileName) => !fileName.endsWith(".jar")
  }
  filtered :+ fatJar -> ("lib/" + fatJar.getName)
}

assemblyMergeStrategy in assembly := {
  case n if n.contains("services") => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", ps @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
