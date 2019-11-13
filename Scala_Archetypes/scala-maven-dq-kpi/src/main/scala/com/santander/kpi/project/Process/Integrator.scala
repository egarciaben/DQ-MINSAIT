package com.santander.kpi.project.Process

import com.santander.kpi.project.Params.LoadConfig
import org.apache.spark.sql.SparkSession

class Integrator (spark: SparkSession, confParams: LoadConfig){

  var dfBuilder = new DataFrameBuilder(spark)

  def run(): Int ={
    var textReader=dfBuilder.csvToDF(confParams.t_file_read)
    textReader.show()


    0
  }
}