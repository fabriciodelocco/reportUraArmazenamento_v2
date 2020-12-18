import org.elasticsearch.spark._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object reportUraArmazenamento_v2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ReportUraArmazenamento").getOrCreate()
    import org.joda.time.{DateTime, Instant, LocalDate}
    import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    DateTimeFormat.forPattern("yyyyMMdd")
    val anteontem =  new DateTime().minusDays(2).withTimeAtStartOfDay().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:SS"))
    val ontem =  new DateTime().minusDays(1).withTimeAtStartOfDay().toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:SS"))
    val hoje = new DateTime().withTimeAtStartOfDay().toString(DateTimeFormat.forPattern("yyyyMMdd"))

    val param = "datahora > \"" + anteontem + "\" and datahora < \"" + ontem + "\" order by datahora asc"

    val uraarmazenamento = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", "srvelkcasprod01")
      .option("es.port", "9200")
      .option("es.net.ssl", "true")
      .option("es.nodes.discovery", "false")
      .option("es.net.http.auth.user", "elastic")
      .option("es.net.http.auth.pass", "2BrR8mEn@Oss")
      .option("es.net.ssl.cert.allow.self.signed", "true")
      .load("ura_armazenamento/_doc")

    uraarmazenamento.createOrReplaceTempView("uraarmazenamento")

    val query = "select * from uraarmazenamento where " + param

    val saida = spark.sql(query)

    val pathoutputfile = "/boarding/uraoutage/reports/uraoutage_"+hoje+".csv"


    saida
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/data/uraoutage/collect/ura_armazenamento/uraoutage_"+hoje+".csv")

    saida.write.
      format("com.springml.spark.sftp").
      option("host", "10.129.251.35").
      option("username", "93730530").
      option("password", "Cl@r0123").
      option("fileType", "csv").
      option("delimiter", ",").
      save(pathoutputfile)

    spark.stop()

  }
}