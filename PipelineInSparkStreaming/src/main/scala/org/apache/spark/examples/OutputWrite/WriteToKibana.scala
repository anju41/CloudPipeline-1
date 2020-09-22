package org.apache.spark.examples.OutputWrite

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.elasticsearch.spark.sql._
import scala.concurrent.{ExecutionContext, Future}


object WriteToKibana {

  def writeTOKibanaIndex(df: DataFrame,TransportIP: String, table_name: String)(implicit xc:ExecutionContext) :Future[Unit]= Future {
    println(s"Writing to Kibanana")

    df.write.
      format("org.elasticsearch.spark.sql")
      .option("es.port","9200")
      .option("es.nodes.discovery","false")
      .option("es.nodes",s"$TransportIP")
      .option("es.index.auto.create","true")
      .option("es.nodes.wan.only","true")
      .option("index.mapping.single_type","false")
      .mode("append").
      save(s"/$table_name/doc")

    Future.successful(())
  }
}
