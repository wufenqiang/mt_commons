package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.latlonUtil

import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData

private object fromLatLon {
  def main (args: Array[String]): Unit = {
    val jsonFile = args(0)
    val lat_target: Double = 38.0d
    val lon_target: Double = 129.0d
    val lat = ArrayOperation.ArithmeticArray(0.0d, 60.0d, 0.01d)
    val lon = ArrayOperation.ArithmeticArray(70.0d, 140.0d, 0.01d)

    val dataType: String = "n1h"
    val key = this.kvIndex(dataType, lat_target, lon_target, lat, lon)
    val idName = this.getIdName(lat_target, lon_target, jsonFile)
    println("lat=" + lat_target + ",lon=" + lon_target + ",idName=" + idName + ",key=" + key)
  }

  def kvIndex (dataType: String, lat_target: Double, lon_target: Double, lat: Array[Double], lon: Array[Double]): String = {
    val latlonStep:Double=0.01d


    val latIndex: Int = ArrayOperation.nearIndex_sequence(lat, lat_target)
    val lonIndex: Int = ArrayOperation.nearIndex_sequence(lon, lon_target)

    dataType.toLowerCase + "_" + latIndex + "_" + lonIndex
  }

  def getIdName(lat:Double,lon:Double,jsonFile:String): String ={

    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    var idNameStr=""
    jsonRdd.foreach(j=>{
      val (idName, timeBegin, timeEnd, timeStep, heightBegin, heightEnd, heightStep, latBegin, latEnd, latStep, lonBegin, lonEnd, lonStep) = Analysis.analysisJsonProperties(j)
      if(latBegin<=lat && lat <= latEnd && lonBegin<=lon && lon<=lonEnd){
        idNameStr=idName
      }
    })
    idNameStr
  }
}
