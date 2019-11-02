package com.santander.kpi.project.Process

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.col

class DataFrameBuilder (val spark: SparkSession) extends LazyLogging{

  def csvToDF(path:String):DataFrame ={
    val df=spark.read.format("csv").
      option("sep",",").
      option("header","true").
      load(path)
    df
  }
}
