name := "reportUraArmazenamento_v2"

version := "0.1"

scalaVersion := "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
// https://mvnrepository.com/artifact/commons-httpclient/commons-httpclient
libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "7.10.0"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.10.0"
// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.10.8"
// https://mvnrepository.com/artifact/com.springml/spark-sftp
libraryDependencies += "com.springml" %% "spark-sftp" % "1.1.5"
