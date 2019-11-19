package com.santander.kpi.project.Process

import com.santander.kpi.project.Params.LoadConfig
import org.apache.spark.sql.SparkSession

class Integrator (spark: SparkSession, confParams: LoadConfig){

  var dfBuilder = new DataFrameBuilder(spark)

  def run(): Int ={
    val textReader=dfBuilder.csvToDF(confParams.t_file_read)
    textReader.show()

    val DFCtesBEI=dfBuilder.csvPipeToDF(confParams.t_det_client_bei)
    DFCtesBEI.show()

    val DFCtesPyme = dfBuilder.csvTabularToDF(confParams.t_det_client_pyme)
    DFCtesPyme.show()

    val DFCtesPart = dfBuilder.csvPipeNoHeaderToDF(confParams.t_det_client_part)
    println("clientes particulares")
    DFCtesPart.show()

    val excelReader=dfBuilder.xlsxToDF(confParams.t_base_global)
    excelReader.show(15)

    val parquetLoader=dfBuilder.parquetToDF(confParams.t_063)
    parquetLoader.show(10)




    val dfSales=dfBuilder.salesList(textReader)
    dfSales.show()

    val dfSalesKPI = dfBuilder.salesKPI(dfSales)
    dfSalesKPI.show()



    0
  }
}