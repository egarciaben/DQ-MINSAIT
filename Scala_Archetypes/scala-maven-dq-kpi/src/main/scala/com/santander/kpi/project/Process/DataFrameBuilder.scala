package com.santander.kpi.project.Process

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import com.santander.kpi.project.InitSpark

class DataFrameBuilder (spark: SparkSession) extends (LazyLogging){

  /*csv-txt separado por comas con encabezados*/
  def csvToDF(path : String):DataFrame ={
    val df=spark.read.format("csv").
      option("sep",",").
      option("header","true").
      load(path)
    df
  }
  /*csv -txt separado por pipelines con encabezados*/
  def csvPipeToDF(path : String):DataFrame ={
    val df=spark.read.format("csv").
      option("sep","|").
      option("header","true").
      load(path)
    df
  }

  /*csv-txt separado por Tabuladores con encabezados*/
  def csvTabularToDF(path : String):DataFrame ={
    val df=spark.read.format("csv").
      option("sep","\t").
      option("header","true").
      load(path)
    df
  }

  /*csv - txt separado por pipelines SIN encabezados*/
  def csvPipeNoHeaderToDF(path : String): DataFrame ={
    val df=spark.read.format("csv").
      option("sep","|").
      option("header","false").
      load(path)
    df
  }




  def xlsxToDF(path : String):DataFrame ={
    val df=spark.read.excel(
      useHeader = true,
      treatEmptyValuesAsNulls = false
    ).load(path)
    df
  }

  def parquetToDF(path : String): DataFrame= {
    val df = spark.read.format("parquet").load(path)
    df
  }



  def salesList(dfSales : DataFrame): DataFrame={
    val df = dfSales.select(
      col("nombre").as("first_name"),
      col("apellido").as("second_name"),
      col("ventas").as("total_sales")
    )
    df
  }

  def salesKPI(dfSalesKPI: DataFrame): DataFrame={
    val df = dfSalesKPI.groupBy("first_name","second_name")
      .agg(sum("total_Sales")).withColumnRenamed("sum(total_Sales)","KPI_Sales")
    df
  }

  /*integracion de clientes Vigentes: funcion que realiza la union de dataframes conjuntando el universo de clientes vigentes*/

  def ctesVigentes(dfBEI : DataFrame, dfPYME : DataFrame, dfPART : DataFrame, dfBMG : DataFrame) : DataFrame={
    val UnivVig = dfBEI.union(dfPYME.union(dfPART.union(dfBMG)))

    UnivVig
  }

}