package com.santander.kpi.project.Process

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._
import com.santander.kpi.project.InitSpark
import org.apache.spark.sql.types.IntegerType

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

  /*Funcion de Lectura de archivos xls*/
  def xlsxToDF(path : String):DataFrame ={
    val df=spark.read.excel(
      useHeader = true,
      treatEmptyValuesAsNulls = false
    ).load(path)
    df
  }

  /*funcion de letura de Parquets*/
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

  /*------------------------UNIVERSO FINAL DE CLIENTES VIGENTES-------------------------*/
def univVigActiv (df : DataFrame, df2 : DataFrame): DataFrame={
  val DFUnivVigAct = df.join(df2,df.col("penumper").cast(IntegerType) === df2.col("ID_CLIENTE_VIG")
  ,"Inner")
    /*.where(df2.col("ID_ACTIVO").contains("1"))*/
  DFUnivVigAct
}

  /*extracto de*/
  def univActivos (df : DataFrame, df2 : DataFrame): DataFrame={
    val DFUnivActivos = df.join(df2,df.col("penumper").cast(IntegerType) === df2.col("ID_CLIENTE_VIG")
      ,"Inner")
    .where(df2.col("ID_ACTIVO").contains("1"))
    DFUnivActivos
  }


  /*--------------------------------TELEFONOS----------------------------------------*/

 /*dataframe que unicamente contiene penumper, pehstamp y pesectel, esta informacion se cruzara con el DF de fechas mas actualizadas y se AGRUPARA por maxima secuencia en un paso posterior*/
  def getActSeq(df : DataFrame) : DataFrame={
    val DFTelSec = df.
      select(
        col("penumper").as("penumper_seq"),
        substring(col("pehstamp"),1,10).as("fechamod_seq"),
        col("pesectel").as("pesectel_seq")
      )
    DFTelSec
  }

  /*agrupado por penumper con max de fecha de actualizacion*/
  def ultimAct(df : DataFrame) : DataFrame={
    val DFSubst = df.select(col("penumper_seq").as("penumper_max"),col("fechamod_seq").as("fechamod_max"))
    val DFUltAct = DFSubst.groupBy("penumper_max").agg(max("fechamod_max").as("fechamod_max"))
    DFUltAct
  }

  /*deteccion de telefono con maxima sequencia*/

  def secuencias(df : DataFrame):DataFrame ={
    val seq = df.select(col("penumper_seq"), col("fechamod_seq")).distinct()
 seq
  }

    def telMaxSeq(df : DataFrame, df2 : DataFrame) : DataFrame={
      val dfMaxSeq = df.join(df2,
        df.col("penumper_max") === df2.col("penumper_seq")
        && df.col("fechamod_max") === df2.col("fechamod_seq")
        ,"inner")

      val DFNumPerTelAct = /*dfMaxSeq.select(
        col("penumper_seq"),
        col("fechamod_seq"),
        col("pesectel_seq")*/
      //dfMaxSeq.groupBy("penumper_seq","fechamod_seq").agg(max("pesectel_seq").as("pesectel_seq"))
        dfMaxSeq.groupBy("penumper_max","fechamod_max").agg(max("pesectel_seq").as("pesectel_seq")).withColumnRenamed("penumper_max","penumper_seq").withColumnRenamed("fechamod_max","fechamod_seq")

      DFNumPerTelAct

    }

  /*
    def telActual(df : DataFrame) : DataFrame={
      val dfTelAct = df.groupBy("penumper_seq")
    }
*/
  def univTel(df : DataFrame) : DataFrame={
    val DFUnivTel = df.
      select(
        col("penumper").as("numper"),
        concat(trim(col("pepretel")),trim(col("penumtel"))).as("TEL"),
        col("pesectel"),
        //col("petiptel"),
        //col("peclatel"),
        substring(col("pehstamp"),1,10).as("FECHAMODTR")
      )
    DFUnivTel
  }

  def finalNumTel(df : DataFrame, df2 : DataFrame) : DataFrame ={
    val joinFinalTel = df.join(df2,
      df.col("penumper_seq") === df2.col("numper")
        && df.col("fechamod_seq")=== df2.col("FECHAMODTR")
      && df.col("pesectel_seq")=== df2.col("pesectel")
      ,"inner")

    val FinalTel = joinFinalTel.select(
      col("numper"),
      col("TEL")
    )
    FinalTel
  }

  /*universo de clientes vigentes con telefonos actualizados*/
  /*el cruce con telefonos se realizara mediante DFVigentesAct.penumper = DFTelefonos.numper*/
  def univCtesVigTel(df : DataFrame, df2 : DataFrame): DataFrame={
    val TotalActivUniv=df.join(df2,df.col("penumper")===df2.col("numper"),"left")
    TotalActivUniv
  }

  def KPI00502Activ(df : DataFrame)={
    val kpi00502 = df.count()
    kpi00502
  }

  def KPI00502ActNoTel(df : DataFrame)={
    val kpi00502 = df.where(col("TEL").isNull).count()
    kpi00502
  }



}
