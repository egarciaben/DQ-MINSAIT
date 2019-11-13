package com.santander.kpi.project.Params

import com.typesafe.config.{Config, ConfigFactory}

class LoadConfig (confOrigen: Config) {
  var config= confOrigen
  var t_file_read : String = ""

  def load(): Int ={
    t_file_read = ConfigFactory.load(config).getString("ParquetReader.Input.t_file_read")
    println(s"despliegue de nombre:--->$t_file_read")
    0
  }
}
