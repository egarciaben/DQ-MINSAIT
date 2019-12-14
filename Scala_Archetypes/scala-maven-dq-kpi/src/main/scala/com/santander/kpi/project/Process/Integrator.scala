package com.santander.kpi.project.Process

import com.santander.kpi.project.Params.LoadConfig
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

class Integrator (spark: SparkSession, confParams: LoadConfig){

  var dfBuilder = new DataFrameBuilder(spark)

  def run(): Int ={
    val textReader=dfBuilder.csvToDF(confParams.t_file_read)
    //textReader.show()

    // se va a comentar
    /*
    val DFCtesBEI=dfBuilder.csvPipeToDF(confParams.t_det_client_bei)
      .select(
        col("ID_CLIENTE").cast(IntegerType).as("ID_CLIENTE_VIG"),
        lit("BEI").as("CARTERA"),
        col("ID_ACTIVO").cast(IntegerType).as("ID_ACTIVO")
      )
    //DFCtesBEI.show()

    val DFCtesPyme = dfBuilder.csvTabularToDF(confParams.t_det_client_pyme)
        .select(
          col("ID_CLIENTE").cast(IntegerType).as("ID_CLIENTE_VIG"),
          lit("PYMES").as("CARTERA"),
          col("ID_ACTIVO").cast(IntegerType).as("ID_ACTIVO")
        )
    //----------------------------->DFCtesPyme.show()

    val DFCtesPart = dfBuilder.csvPipeNoHeaderToDF(confParams.t_det_client_part)
        .select(
          col("_c0").as("ID_CLIENTE").cast(IntegerType).as("ID_CLIENTE_VIG"),
          lit("PARTICULARES").as("CARTERA"),
          col("_c12").as("ID_ACTIVO").cast(IntegerType).as("ID_ACTIVO")
        )
    println("clientes particulares")
    //----------------------------->DFCtesPart.show()

    val DFCtesBMG=dfBuilder.xlsxToDF(confParams.t_base_global)
        .select(
          col("BUC (MX)").as("ID_CLIENTE").cast(IntegerType).as("ID_CLIENTE_VIG"),
          lit("BMG").as("CARTERA"),
          lit("1").as("ID_ACTIVO").cast(IntegerType).as("ID_ACTIVO")
        )
    println("Clientes BMG")
    //----------------------------->DFCtesBMG.show(15)

    val DFVigentes = dfBuilder.ctesVigentes(DFCtesBEI, DFCtesPyme, DFCtesPart, DFCtesBMG)

    /*extracto productivo*/
    /*
    val DFHeader = DFVigentes.select(lit("ID_CLIENTE").as("ID_CLIENTE")).distinct()

    val DFProd_pre = DFVigentes.select(
      col("ID_CLIENTE")
    )

    val DFProd =DFHeader.union(DFProd_pre)
    //----------------------------->DFProd.show()
*/
    /*
    Validacion de unicidad de clientes
    println("conteoClientes")
    val conteoclientes = DFProd_pre.count()
    println(conteoclientes)
    println("conteoClientesDistinct")
    val conteoclientesDistinct = DFProd_pre.distinct().count()
    println(conteoclientesDistinct)
*/
    //println("escribiendo archivo de salida...")
    //DFProd.coalesce(1).write.csv("src/main/scala/com/santander/kpi/project/out/clietes_vigentes")
    //println("Generado")

    //val Dist = DFVigentes.select("CARTERA").distinct()
    //----------------------------->println("Distinct Cartera")
    //Dist.show()

    //println("validacion de esquema(tipos de dato)")
    //DFVigentes.printSchema()




    val parquetLoader=dfBuilder.parquetToDF(confParams.t_063)
    //------*parquetLoader.show(10)

    val pedt001 =dfBuilder.parquetToDF(confParams.t_pedt001)
    println("conteo de personas pedt001")
/*
    val counteopdt01 = pedt001.select(col("penumper")).count()
    println(counteopdt01)
    println("conteo de distinct personas pedt001")
    val counteoDistinctpdt01 = pedt001.select(col("penumper")).distinct().count()
    println(counteoDistinctpdt01)
*/


    println("conteo de cliente vigentes")
    /*conteo de clientes vigentes tras cruce con insumos*/
    val DFVigentesAct = dfBuilder.univVigActiv(pedt001, DFVigentes)
    val cuentaDFVigentesAct = DFVigentesAct.count()


//  println("conteo de clientes activos")
    val DFActivos = dfBuilder.univActivos(pedt001,DFVigentes)
    //val cuentaDFActivo=DFActivos.count()

    //println(cuentaDFActivo)


    //DFVigentesAct.show()
/*.......................................<TELEFONOS>..............................................*/
/*Integracion de DataFrame con telefonos actualizados*/
    val pedt023 =dfBuilder.parquetToDF(confParams.t_pedt023)
/*Ultima Actualizacion de Telefono*/

    println("extrae personas, fechas y secuencias")
    val DFSecuencias=dfBuilder.getActSeq(pedt023)

    //DFSecuencias.show()

    val DFUltAct=dfBuilder.ultimAct(DFSecuencias)
/*
    println("ultima actualizacion de telefono")
    println(DFUltAct.count())
    println("aplicando distinct")
    println(DFUltAct.distinct().count())
 */
    //DFUltAct.show(100)

    val DFUnivSeq = dfBuilder.telMaxSeq(DFUltAct,DFSecuencias)
  //  println("join de telefonos")
//    println(DFUnivSeq.count())


    val DFUnivTel = dfBuilder.univTel(pedt023)

    /*  println("buscando cliente multiplicado")

      val desglose = DFUnivTel.select(col("numper"), col("TEL"), col("pesectel"), col("FECHAMODTR"))
        .where(col("numper") === "38670217")

      desglose.show(700)
  */
    */    //termina el comentado

    var DFTelefonos : Dataset[Row] = null
    if(confParams.local){
      DFTelefonos = dfBuilder.parquetToDF(confParams.t_telefonos)
    }else {
      //DFTelefonos = dfBuilder.finalNumTel(DFUnivSeq, DFUnivTel).distinct()

      println("Escribiendo el archivo parquet de telefonos...")
      DFTelefonos.write.parquet(confParams.t_file_parquet+"/t_telefonos_dev")
      println("Fin de la escritura del archivo parquet de telefonos...")
    }


/*.......................................</TELEFONOS>...................................................*/
/*........................................<PERSONAS>....................................................*/

    var DFClientesActivos : Dataset[Row] = null

    if(confParams.local){
      DFClientesActivos = dfBuilder.parquetToDF(confParams.t_clientes_activos)
    }else{
      //DFClientesActivos = dfBuilder.univCtesVigTel(DFVigentesAct,DFTelefonos)
      println("resultado final clientes vigentes cruzando con matriz de telefonos actualizados")
      //DFClientesActivos.show()

      println("Escribiendo el archivo de clientes activos")
      DFClientesActivos.write.parquet(confParams.t_file_parquet+"/t_clientes_activos_dev")
      println("Fin de la escritura del archivo de clientes activos")
    }



//val repetidos = DFClientesActivos.groupBy("penumper").agg(count("TEL").as("repetido"))
//val rep = repetidos.select(col("penumper"),col("repetido")).where(col("repetido") > 1)
 //   rep.show()

/*println(DFClientesActivos.count())*/

/* desde aqui comienza los comentarios
    println("************* K.BMX.PE.00005.O.002 VIGENTES****************")

    val kpi1Vig = dfBuilder.KPI00502Vig(DFClientesActivos)
    println("conteo de empleados Activos totales")
    println(kpi1Vig)

    val kpi1VigNoTel = dfBuilder.KPI00502VigNoTel(DFClientesActivos)
    println("conteo de empleados sin telefono")
    println(kpi1VigNoTel)

    println("************* K.BMX.PE.00005.O.002 ACTIVOS****************")
    val kpi1Activ = dfBuilder.KPI00502Activ(DFClientesActivos)
    println("conteo de empleados Activos totales")
    println(kpi1Activ)

    val kpi1ActivNoTel = dfBuilder.KPI00502ActNoTel(DFClientesActivos)
    println("conteo de empleados sin telefono")
    println(kpi1ActivNoTel.count())

    val dfSales=dfBuilder.salesList(textReader)
    //------*dfSales.show()

    val dfSalesKPI = dfBuilder.salesKPI(dfSales)
    //------*dfSalesKPI.show()


    /*2.-# de clientes con el dato "teléfono" con formato incorrecto/ total de clientes activos*/
    /*K.BMX.PE.00005.O.003*/

    println("************* K.BMX.PE.00005.O.003 VIGENTES****************")

    println("KPI00503 Format IncorrectNum Vigentes:" )
    val kpi00503Num = dfBuilder.KPI00503FormatIncorrectNum(DFClientesActivos)
    println(kpi00503Num)

    println("KPI00503 Format IncorrectDen Vigentes:" )
    val DenVig = dfBuilder.KPI00503FormatIncorrectDen(DFClientesActivos)
    println(DenVig)

    println("************* K.BMX.PE.00005.O.003 ACTIVOS****************")

    println("KPI00503 Format IncorrectNum Activos:" )
    val kpi00503NumAct = dfBuilder.KPI00503FormatIncorrectNumACT(DFClientesActivos)
    println(kpi00503NumAct.count())



    println("KPI00503 Format IncorrectDen Activos:" )
    val DenAct = dfBuilder.KPI00503FormatIncorrectDenACT(DFClientesActivos)
    println(DenAct)



    println("************* K.BMX.PE.00005.O.004 # de clientes con el dato \"teléfono\" con más de 5 números consecutivos idénticos / total de clientes activos****************")
    println("KPI00504 Numeros Identicos Vigentes:" )
    val kpi00504NumVig = dfBuilder.KPI00504NumerosIdenticosNumVig(DFClientesActivos)
    println(kpi00504NumVig)
    println(s"Denominador de vigentes : $DenVig")

    println("KPI00504 Numeros Identicos Activos:" )
    val kpi00504NumAct = dfBuilder.KPI00504NumerosIdenticosNumAct(DFClientesActivos)
    println(kpi00504NumAct.count())
    println(s"Denominador de Activos : $DenAct")


    /****4.-# de clientes con el dato teléfono con números secuenciales / total de clientes activos***/
    /****K.BMX.PE.00005.O.006*****/
    println("/****4.-# de clientes con el dato teléfono con números secuenciales / total de clientes activos***/\n/****K.BMX.PE.00005.O.006*****/")
    val kpi506NumVig = dfBuilder.KPI00506NumerosSecuencialesNumVig(DFClientesActivos)
    //kpi506NumVig.show()
    //println(kpi506NumVig.count())

    val kpi506NumAct = dfBuilder.KPI00506NumerosSecuencialesNumAct(DFClientesActivos)
    //println(kpi506NumAct.count())

    /****5.-# de clientes con el dato teléfono repetido / total de clientes activos***/
    /****K.BMX.PE.00005.O.007****/

    val query507 = dfBuilder.Query507DF(DFClientesActivos)
    val kpi507NumVig = dfBuilder.KPI00507TelefonoRepetidoNumVig(DFClientesActivos,query507)
    val kpi507NumAct = dfBuilder.KPIsClientesActivos(kpi507NumVig)
    val kpi507Detalle = dfBuilder.KPI00507TelefonoRepetidoDetalle(kpi507NumAct)


     println("********KPI507***********")
   // kpi507NumVig.show()
   // println(kpi507NumVig.count())
    //kpi507NumAct.show()
   // println(kpi507NumAct.count())
   // kpi507Detalle.show()


 terminan los comentarios*/

    /****6.-# de clientes con el dato "correo electrónico" nulo/ total de clientes activos***/
    /******K.BMX.PE.00006.O.002*******/
      val correoNulo = dfBuilder.KPI00602CorreoNuloVig(DFClientesActivos)
    println("K.BMX.PE.00006.O.002")
   // correoNulo.show()
    val correoNulVig = correoNulo.count()
   // println(correoNulVig)

    val correoNuloAct = dfBuilder.KPIsClientesActivos(correoNulo)
   // correoNuloAct.show()
    val correoNulAct = correoNuloAct.count()
    //println(correoNulAct)

    /****7.-# de clientes con el dato "correo electrónico" con formato incorrecto/ total de clientes activos***/
    /****K.BMX.PE.00006.O.003*****/

    val kpi6003 = dfBuilder.KPI00603PruebaNum(DFClientesActivos)
    val kpidominios = dfBuilder.KPI00603Dominios(dfBuilder.csvToDF(confParams.t_dominios))

    println("kpi6003 ")
    kpi6003.show()
    kpidominios.show()
    println("te amo kike ")
    val kpi6003NumVig=dfBuilder.KPI00603CorreoFormatIncorrectVig(kpi6003,kpidominios)
    println(kpi6003.count())



    dfBuilder.KPI00605Query64(DFClientesActivos).show()

/*
//Escritura de archivos CSVs para evidencia de KPIs

    val detalleActivKPI00502 = dfBuilder.PenumperTel(kpi1ActivNoTel)
    detalleActivKPI00502.coalesce(1).write.format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(confParams.t_file_parquet+"/Detalles/K_BMX_PE_00005_O_002_Activos")

    val detalleActivKPI00503 = dfBuilder.PenumperTel(kpi00503NumAct)
    detalleActivKPI00503.coalesce(1).write.format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(confParams.t_file_parquet+"/Detalles/K_BMX_PE_00005_O_003_Activos")

    val detalleActivKPI00504 = dfBuilder.PenumperTel(kpi00504NumAct)
    detalleActivKPI00504.coalesce(1).write.format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(confParams.t_file_parquet+"/Detalles/K_BMX_PE_00005_O_004_Activos")

      val detalleActivKPI00506 = dfBuilder.PenumperTel(kpi506NumAct)
      detalleActivKPI00506.coalesce(1).write.format("csv")
        .option("header","true")
        .mode("overwrite")
        .save(confParams.t_file_parquet+"/Detalles/K_BMX_PE_00005_O_006_Activos")

        kpi507Detalle.coalesce(1).write.format("csv")
        .option("header","true")
        .mode("overwrite")
        .save(confParams.t_file_parquet+"/Detalles/K_BMX_PE_00005_O_007_Activos")


    //Generacion y escritura del archivo de reporte de kpis

    val rowData = List(
      ("K.BMX.PE.00005.O.002", kpi1VigNoTel,DenVig,kpi1ActivNoTel.count(),DenAct),
      ("K.BMX.PE.00005.O.003", kpi00503Num,DenVig,kpi00503NumAct.count(),DenAct),
      ("K.BMX.PE.00005.O.004", kpi00504NumVig,DenVig,kpi00504NumAct.count(),DenAct),
      ("K.BMX.PE.00005.O.006", kpi506NumVig.count(),DenVig,kpi506NumAct.count(),DenAct),
      ("K.BMX.PE.00005.O.007", kpi507NumVig.count(),DenVig,kpi507NumAct.count(),DenAct),
      ("K.BMX.PE.00006.O.002", correoNulVig,DenVig,correoNulAct,DenAct)

    )

    val scTest= dfBuilder.createReporteFinalDF( rowData)

    scTest.show();

    scTest.coalesce(1).write.format("csv")
      .option("header","true")
      .mode("overwrite")
      .save(confParams.t_file_parquet+"/Detalles/ReporteFinal")
*/

    0
  }
}