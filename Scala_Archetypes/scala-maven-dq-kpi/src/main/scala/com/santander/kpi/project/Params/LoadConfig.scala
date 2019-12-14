package com.santander.kpi.project.Params

import com.typesafe.config.{Config, ConfigFactory}

class LoadConfig (confOrigen: Config) {
  var config= confOrigen
  var t_file_read : String = ""
  var t_base_global: String = ""
  var t_063: String = ""
  var t_det_client_bei : String = ""
  var t_det_client_pyme : String = ""
  var t_det_client_part : String = ""
  var t_pedt001 : String = ""
  var t_pedt023 : String = ""
  var t_telefonos : String = ""
  var t_clientes_activos : String = ""
  var t_file_parquet : String = ""
  var local : Boolean = true
  var t_pedt115 : String = ""
  var t_dominios: String = ""

  def load(): Int ={
    t_file_read = ConfigFactory.load(config).getString("ParquetReader.Input.t_file_read")
    println(s"despliegue de nombre:--->$t_file_read")

    t_base_global = ConfigFactory.load(config).getString("ParquetReader.Input.t_base_global")
    println(s"ruta case global:--->$t_base_global")

    t_063 = ConfigFactory.load(config).getString("ParquetReader.Input.t_063")
    println(s"ruta t_063: --->$t_063")

    t_det_client_bei = ConfigFactory.load(config).getString("ParquetReader.Input.t_detalle_clientes_bei")
    println(s"ruta clientes bei: --->$t_det_client_bei")

    t_det_client_pyme = ConfigFactory.load(config).getString("ParquetReader.Input.t_detalle_clientes_pyme")
    println(s"ruta clientes pyme: --->$t_det_client_pyme")

    t_det_client_part = ConfigFactory.load(config).getString("ParquetReader.Input.t_detalle_ctes_particulares")
    println(s"ruta clientes particulares:--->$t_det_client_part")

    t_pedt001 = ConfigFactory.load(config).getString("ParquetReader.Input.t_pdt001")
    println(s"ruta clientes particulares:--->$t_pedt001")

    t_pedt023 = ConfigFactory.load(config).getString("ParquetReader.Input.t_pdt023")
    println(s"ruta clientes particulares:--->$t_pedt023")

    t_file_parquet = ConfigFactory.load(config).getString("ParquetReader.Output.t_file_parquet")
    println(s"ruta de salida de archivos: ---> $t_file_parquet")

    t_pedt115 = ConfigFactory.load(config).getString("ParquetReader.Input.t_pedt115")

    t_dominios = ConfigFactory.load(config).getString("ParquetReader.Input.t_dominios")

    if(local){

      t_telefonos = ConfigFactory.load(config).getString("ParquetReader.Input.t_telefonos")
      println(s"ruta telefonos:--->$t_telefonos")

      t_clientes_activos = ConfigFactory.load(config).getString("ParquetReader.Input.t_clientes_activos")
      println(s"ruta clientes activos:--->$t_clientes_activos")
    }




    0
  }
}