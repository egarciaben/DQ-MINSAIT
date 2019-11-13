package com.santander.kpi.project

import com.santander.kpi.project.Params.LoadConfig
import com.santander.kpi.project.Process.Integrator
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql

object KPIEngineTrail extends InitSpark{
  def main(args: Array[String]) {

    val config = ConfigFactory.load()
    val loadC = new LoadConfig(config)
    loadC.load()

    val dfIntegrator = new Integrator(spark,loadC)
    dfIntegrator.run()


  }
}
