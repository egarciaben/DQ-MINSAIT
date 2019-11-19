package com.santander.kpi.project.Process

import com.santander.kpi.project.Params.LoadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class Integrator (spark: SparkSession, confParams: LoadConfig){

  var dfBuilder = new DataFrameBuilder(spark)

  def run(): Int ={
    val textReader=dfBuilder.csvToDF(confParams.t_file_read)
    textReader.show()

    val DFCtesBEI=dfBuilder.csvPipeToDF(confParams.t_det_client_bei)
      .select(
        col("ID_CLIENTE"),
        lit("BEI").as("CARTERA"),
        col("ID_ACTIVO")
      )
    DFCtesBEI.show()

    val DFCtesPyme = dfBuilder.csvTabularToDF(confParams.t_det_client_pyme)
        .select(
          col("ID_CLIENTE"),
          lit("PYMES").as("CARTERA"),
          col("ID_ACTIVO")
        )
    DFCtesPyme.show()

    val DFCtesPart = dfBuilder.csvPipeNoHeaderToDF(confParams.t_det_client_part)
        .select(
          col("_c0").as("ID_CLIENTE"),
          lit("PARTICULARES").as("CARTERA"),
          col("_c12").as("ID_ACTIVO")
        )
    println("clientes particulares")
    DFCtesPart.show()

    val DFCtesBMG=dfBuilder.xlsxToDF(confParams.t_base_global)
        .select(
          col("BUC (MX)").as("ID_CLIENTE"),
          lit("BMG").as("CARTERA"),
          lit("1").as("ID_ACTIVO")
        )
    println("Clientes BMG")
    DFCtesBMG.show(15)

    val DFVigentes = dfBuilder.ctesVigentes(DFCtesBEI, DFCtesPyme, DFCtesPart, DFCtesBMG)
    println("Clientes Vigentes:")
    DFVigentes.show(15)

    val Dist = DFVigentes.select("CARTERA").distinct()
    println("Distinct Cartera")
    //Dist.show()

println("validacion de esquema(tipos de dato)")
    DFVigentes.printSchema()




    val parquetLoader=dfBuilder.parquetToDF(confParams.t_063)
    //------*parquetLoader.show(10)




    val dfSales=dfBuilder.salesList(textReader)
    //------*dfSales.show()

    val dfSalesKPI = dfBuilder.salesKPI(dfSales)
    //------*dfSalesKPI.show()



    0
  }
}