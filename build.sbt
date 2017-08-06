name := "kafka-delivery-gurnatee"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
                      "org.apache.kafka" % "kafka-clients" % "0.11.0.0" ,
                      "org.slf4j"              % "slf4j-log4j12" % "1.7.25"

)