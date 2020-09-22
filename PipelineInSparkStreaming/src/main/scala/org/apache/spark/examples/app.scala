package org.apache.spark.examples
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.nio.file.{Files, Paths}
import org.elasticsearch.spark.sql._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, DataFrame, Dataset, ForeachWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import com.datastax.spark.connector._
import org.apache.spark.examples.sparkSession.ConnectSession.getSparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.get_json_object

object app {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Connecting to Spark")
      .set("spark.dynamicAllocation.enabled","true")
      .set("spark.dynamicAllocation.testing","true")
      .set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite","true")
      .set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite","true")
      .set("spark.cassandra.auth.username","cluster1-superuser")
      .set("spark.cassandra.auth.password","")

   /* val spark = SparkSession.builder().appName("cassandratest").config(conf).master("local").config("spark.es.nodes","10.200.2.121").config("spark.es.port","9200").config("es.nodes.wan.only","true").getOrCreate()

    import spark.implicits._
   spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "34.86.183.222:9094")
      .option("multiLine","true")
      .option("mode", "PERMISSIVE")
      .option("subscribe", "nodejs.mydb.emp")
      .option("startingOffsets", "latest") //latest
      .load()

    val structureSchema = new StructType()
      .add("id", IntegerType)
      .add("Name", StringType)
      .add("Salary", IntegerType)


    df.select(col("topic").cast("String"), col("value").cast("String")).writeStream
      .foreachBatch {
        (df: Dataset[Row], _: Long) =>
          df.show(false)
          val DF = df.select(from_json(col("value"), structureSchema).as("data"))
            .select("data.*").show()
      }.start().awaitTermination()

   // val Data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/tech/emp.csv")
    //Data.show()
    //Data.saveToEs("mypoc/_doc")

   /*
    Data.select(to_avro(struct("*")) as "value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "34.86.183.222:9094")
      .option("kafka.request.required.acks", "1")
      .option("topic", "topic1-emp")
      .save()

   val Data1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/tech/dept.csv")

    Data1.show()
    Data1.select(to_avro(struct("*")) as "value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", ":9094")
      .option("kafka.request.required.acks", "1")
      .option("topic", "topic1-dept")
      .save() */

    /*Data.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "emp", "keyspace" -> "mydb"))
      .option("ttl", "1000") //Use 1000 as the TTL
      .mode("APPEND")
      .save() */



    //val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "34.86.183.222:9094").option("subscribe", "test").option("multiLine", true).option("startingOffsets", "latest").load()


  // df.select(col("topic").cast("String"), col("value").cast("String")).writeStream.foreachBatch {  (df: Dataset[Row], _: Long) => df.show() }.start().awaitTermination()


    */

    val spark = SparkSession.builder().master("local").appName("WriteToBigquery").config(conf).config("spark.es.nodes.discovery", "false").config("spark.es.cluster.name","elasticsearch").config("spark.es.index.auto.create","true").config("spark.es.nodes.discovery", "false").config("spark.es.nodes","34.86.214.189").config("spark.es.port","9200").config("es.nodes.wan.only","true").getOrCreate()
    import spark.implicits._

    val Data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/home/tech/emp.csv")

    //val Data = Seq((8, "bat"),(64, "mouse"),(-27, "horse")).toDF("number", "word")

    Data.write.format("org.elasticsearch.spark.sql").option("es.port","9200").option("es.nodes.discovery","false").option("es.nodes","10.200.2.121").option("es.index.auto.create","true").option("es.cluster.name","elasticsearch").option("es.nodes.wan.only","true").option("index.mapping.single_type","false").mode("append").save("/mypoc/emp")



  }
}
