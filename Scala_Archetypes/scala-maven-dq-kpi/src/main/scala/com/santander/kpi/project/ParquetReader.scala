package com.santander.kpi.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame,SQLContext}

object ParquetReader {
def main(args: Array[String])={
  val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("ParquetRader")
  //val sc: SparkContext = new SparkContext(conf)
  //val sqlContext: SQLContext = new SQLContext(sc)
  //writeParquet(sc, sqlContext)
  //readParquet(sqlContext)
}

  /*def writeParquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    val rdd = {
      sqlContext.read.format("csv").option("header", "true").load("/src/main/InputFiles/origen.csv")
    }
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    // Write file to parquet
    df.write.parquet("origen.parquet")
  }*/

  /*
  def readParquet(sqlContext: SQLContext) = {
    // read back parquet to DF
    val newDataDF = sqlContext.read.parquet("origen.parquet")
    // show contents
    newDataDF.show()
  }
  */
}
