package com.santander.kpi.project.Process

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{substring_index, _}
import com.crealytics.spark.excel._
import com.santander.kpi.project.InitSpark
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

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

  /*csv-txt separado por Tabuladores sin encabezados*/
  def txtTabularToDF(path : String):DataFrame ={
    val df=spark.read.format("csv").
      option("sep","\u0001").
      option("header","false").
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
    val kpi00502 = df.where(col("ID_ACTIVO")==="1").count()
    kpi00502
  }

  def KPI00502ActNoTel(df : DataFrame) : DataFrame={
    val kpi00502 = df.where(col("TEL").isNull
    && col("ID_ACTIVO")==="1"
    )
    kpi00502
  }

  def KPI00502Vig(df : DataFrame)={
    val kpi00502 = df.count()
    kpi00502
  }

  def KPI00502VigNoTel(df : DataFrame)={
    val kpi00502 = df.where(col("TEL").isNull).count()
    kpi00502
  }

  /*2.-# de clientes con el dato "teléfono" con formato incorrecto/ total de clientes activos*/
  /*K.BMX.PE.00005.O.003*/

  def KPI00503FormatIncorrectNum(df : DataFrame): Long={
    val kpi00503 = df.where(col("TEL").isNotNull
      && length(col("TEL") ).notEqual(10)).count()

    kpi00503
  }

  def KPI00503FormatIncorrectNumACT(df : DataFrame): DataFrame={
    val kpi00503 = df.where(col("TEL").isNotNull
      && length(col("TEL") ).notEqual(10)
      && col("ID_ACTIVO")==="1")

    kpi00503
  }

  def KPI00503FormatIncorrectDen (df : DataFrame): Long={
    val kpi00503 = df.select("penumper").distinct().count()
    kpi00503
  }

  def KPI00503FormatIncorrectDenACT (df : DataFrame): Long={
    val kpi00503 = df.where(col("ID_ACTIVO")==="1")
      .count()
    kpi00503
  }

  // 3.-# de clientes con el dato "teléfono" con más de 5 números consecutivos idénticos / total de clientes activos
  //K.BMX.PE.00005.O.004

  def KPI00504NumerosIdenticosNumVig(df:DataFrame): Long={
    val pattern = "(1{5,}|2{5,}|3{5,}|4{5,}|5{5,}|6{5,}|7{5,}|8{5,}|9{5,}|0{5,})"
    val kpi504 = df.withColumn("TEL34", regexp_extract(col("TEL"),pattern,1)
    ).where(length(col("TEL34")) > 0
    ).count()
    kpi504
  }

  def KPI00504NumerosIdenticosNumAct(df:DataFrame): DataFrame={
    val pattern = "(1{5,}|2{5,}|3{5,}|4{5,}|5{5,}|6{5,}|7{5,}|8{5,}|9{5,}|0{5,})"
    val kpi504 = df.withColumn("TEL34", regexp_extract(col("TEL"),pattern,1)
    ).where(length(col("TEL34")) > 0
      && col("ID_ACTIVO") === "1"
    )
    kpi504
  }

  def PenumperTel(df:DataFrame): DataFrame={
    val penumperTel = df.select(col("penumper"),col("TEL"))
    penumperTel
  }


  //#4.- # de clientes con el dato teléfono con números secuenciales / total de clientes activos
  //K.BMX.PE.00005.O.006

  def KPI00506NumerosSecuencialesNumVig(df:DataFrame): DataFrame={
    val pattern= "(1234567|2345678|3456789|4567890|0123456)\\d"
    val kpi506= df.withColumn("TEL34",regexp_extract(col("TEL"),pattern,1))
      .where(
      length(col("TEL34")) > 0
      )

    kpi506
  }

  def KPI00506NumerosSecuencialesNumAct(df:DataFrame): DataFrame={
    val pattern= "(1234567|2345678|3456789|4567890|0123456)\\d"
    val kpi506= df.withColumn("TEL34",regexp_extract(col("TEL"),pattern,1))
      .where(
        length(col("TEL34")) > 0 &&
        col("ID_ACTIVO") === "1"
      )

    kpi506
  }

  /****5.-# de clientes con el dato teléfono repetido / total de clientes activos***/
  /****K.BMX.PE.00005.O.007****/

  def Query507DF(df:DataFrame) : DataFrame ={
    val query507 = df.groupBy("TEL")
      .agg(count("TEL")).withColumnRenamed("count(TEL)","count_tel")

    query507
  }

  def KPI00507TelefonoRepetidoNumVig(dfClientes: DataFrame,dfQuery507:DataFrame): DataFrame ={
    val kpi507 = dfQuery507.where(col("count_tel") >= 2)
    val kpi507Alias = kpi507.as("dfKpi507")
    val clienteActDF = dfClientes.as("dfClientes")
    val ar = clienteActDF.join(kpi507Alias,col("dfClientes.TEL") === col("dfKpi507.TEL")
      , "inner")
    ar
  }

  def KPI00507TelefonoRepetidoDetalle(df: DataFrame): DataFrame ={
    val kpi507 = df.select(col("penumper"),col("dfClientes.TEL"),col("count_tel"))
        .orderBy(col("dfClientes.TEL"))
    kpi507
  }

  def KPIsClientesActivos(df: DataFrame): DataFrame ={
    val kpiClientesActivos = df.where(col("ID_ACTIVO") ==="1")
    kpiClientesActivos
  }


  /****6.-# de clientes con el dato "correo electrónico" nulo/ total de clientes activos***/
  /******K.BMX.PE.00006.O.002*******/
    def KPI00602CorreoNuloVig(df:DataFrame): DataFrame={
      val kpi602 = df.where(col("peparme1").isNull ||
      col("peparme2").isNull)
      kpi602
    }

  def EvidenciaCorreos (df :DataFrame):DataFrame ={
    val evi = df.select("penumper","peparme1","peparme2")
    evi
  }

  /****7.-# de clientes con el dato "correo electrónico" con formato incorrecto/ total de clientes activos***/
  /****K.BMX.PE.00006.O.003*****/

  def KPI00603PruebaNum(df:DataFrame):DataFrame = {
    /*val pruebaNum = df.select(
      col("penumper"),
      col("peparme1"),
      col("peparme2"),
      substring_index(col("peparme2"),".",1).alias("prueba1"),
      substring_index(col("peparme2"),".",2).alias("prueba2"),
      substring_index(col("peparme2"),".",3).alias("prueba3"),
      substring_index(col("peparme2"),".",4).alias("prueba4")
    )*/

    val pruebaNum = df
      .withColumn("Prueba1", split(col("peparme2"),"\\."))
      .select(
        col("penumper"),
        col("peparme1"),
        col("peparme2"),
        col("Prueba1").getItem(0).as("Prueba1"),
              col("Prueba1").getItem(1).as("Prueba2"),
              col("Prueba1").getItem(2).as("Prueba3"),
              col("Prueba1").getItem(3).as("Prueba4")

      )
    pruebaNum
  }

  def KPI00603Dominios(df:DataFrame): DataFrame={
    val domain = df.withColumn("DOMINIO",regexp_replace(col("DOMINIO"),"\\.+",""))
    val ODomain = domain.select(col("DOMINIO").as("prueba1_dom"), col("DOMINIO").as("prueba2_dom"), col("DOMINIO").as("prueba3_dom"), col("DOMINIO").as("prueba4_dom"))
    ODomain
  }

  def KPI00603CorreoFormatIncorrectVig(df_pruebaNum:DataFrame,df_dominios:DataFrame):DataFrame={
    val DFMatchDomain = df_pruebaNum.join(
      df_dominios,
        trim(lower(df_pruebaNum.col("prueba2"))) === trim(lower(df_dominios.col("prueba2_dom")))
          /*&&
          trim(lower(df_pruebaNum.col("prueba3"))) === trim(lower(df_dominios.col("prueba3_dom"))) &&
            trim(lower(df_pruebaNum.col("prueba4"))) === trim(lower(df_dominios.col("prueba4_dom")))*/
      ,"left"
    ).where(col("prueba2_dom").isNull)
    DFMatchDomain
  }


  /*****clientes con correo repetido******/
  /****K.BMX.PE.00006.O.004****/

    def KPI00605Query64 (df : DataFrame): DataFrame = {
      val query64 = df.groupBy(col("penumper"),concat(col("peparme1"),col("peparme2")))
        .agg(count( col("penumper"))).withColumnRenamed("count(penumper)","Contador")
        .where(
          col("Contador") === col("Contador")
        )

      val queryR = query64.where(col("Contador") >= 2)

      query64
    }





  //Creacion del DF para la creacion del archivo de reporte.

  private def asRows[U](values: List[U]): List[Row] = {
    values.map {
      case x: Row     => x.asInstanceOf[Row]
      case y: Product => Row(y.productIterator.toList: _*)
      case a          => Row(a)
    }
  }

  private def asSchema[U <: Product](fields: List[U]): List[StructField] = {
    fields.map {
      case x: StructField => x.asInstanceOf[StructField]
      case (name: String, dataType: DataType, nullable: Boolean) =>
        StructField(
          name,
          dataType,
          nullable
        )
    }
  }

  /**
   *
   * @param rowData
   * @tparam U
   * @return
   */
  def createReporteFinalDF[U <: Product](rowData: List[U]): DataFrame = {
    val schema = List(
      ("KPI_Name", StringType, false),
      ("Numerador_Vig", LongType, false),
      ("Denominador_Vig", LongType, false),
      ("Numerador_Act", LongType, false),
      ("Denominador_Act", LongType, false)
    )
   val reporte = spark.createDataFrame(
      spark.sparkContext.parallelize(asRows(rowData)),
      StructType(asSchema(schema))
    )

    reporte
  }


}
