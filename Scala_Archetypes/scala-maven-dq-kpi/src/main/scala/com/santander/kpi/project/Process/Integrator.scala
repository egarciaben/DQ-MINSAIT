package com.santander.kpi.project.Process

import com.santander.kpi.project.Params.LoadConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

class Integrator (spark: SparkSession, confParams: LoadConfig){

  var dfBuilder = new DataFrameBuilder(spark)

  def run(): Int ={
    val textReader=dfBuilder.csvToDF(confParams.t_file_read)
    //textReader.show()

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
    val DFTelefonos = dfBuilder.finalNumTel(DFUnivSeq,DFUnivTel)
    //DFTelefonos.show()

/*.......................................</TELEFONOS>...................................................*/
/*........................................<PERSONAS>....................................................*/

val DFClientesActivos = dfBuilder.univCtesVigTel(DFVigentesAct,DFTelefonos)
    println("resultado final clientes vigentes cruzando con matriz de telefonos actualizados")
    //DFClientesActivos.show()

//val repetidos = DFClientesActivos.groupBy("penumper").agg(count("TEL").as("repetido"))
//val rep = repetidos.select(col("penumper"),col("repetido")).where(col("repetido") > 1)
 //   rep.show()

/*println(DFClientesActivos.count())*/


    val kpi1Activ = dfBuilder.KPI00502Activ(DFClientesActivos)
    println("conteo de empleados Activos totales")
    println(kpi1Activ)

    val kpi1ActivNoTel = dfBuilder.KPI00502ActNoTel(DFClientesActivos)
    println("conteo de empleados sin telefono")
    println(kpi1ActivNoTel)



    val dfSales=dfBuilder.salesList(textReader)
    //------*dfSales.show()

    val dfSalesKPI = dfBuilder.salesKPI(dfSales)
    //------*dfSalesKPI.show()


    0
  }
}