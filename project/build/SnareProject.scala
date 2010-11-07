import sbt._
import Process._

class CrochetProject(info: ProjectInfo) extends DefaultProject(info) {

	// HttpClient
	lazy val com_http_client = "commons-httpclient" % "commons-httpclient" % "3.1"
	
	// Jetty
	lazy val jetty = "org.mortbay.jetty" % "jetty" % "6.1.9"
	
	// MongoDB
	lazy val mongodb = "mongodb" % "mongodb" % "2.3" from "http://cloud.github.com/downloads/mongodb/mongo-java-driver/mongo-2.3.jar"
	
	// Crochet 
	lazy val crochet = "crochet" % "crochet" % "2.8.0-0.1.6" from "http://cloud.github.com/downloads/xllora/Crochet/crochet_2.8.0-0.1.6.jar"
	
	// Testing facilities
	lazy val scalatest = "org.scalatest" % "scalatest" % "1.0"
	lazy val scalacheck = "org.scala-tools.testing" % "scalacheck_2.7.7" % "1.6"
	lazy val specs = "org.scala-tools.testing" % "specs" % "1.6.5" from "http://specs.googlecode.com/files/specs_2.8.0-1.6.5.jar"
	
}