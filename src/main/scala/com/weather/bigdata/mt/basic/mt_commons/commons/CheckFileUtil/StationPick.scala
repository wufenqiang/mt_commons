package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil

import com.weather.bigdata.it.utils.operation.ArrayOperation

object StationPick {
  def main(args:Array[String]):Unit={
    //    val AO:ArrayOperation=new ArrayOperation
    val lat = ArrayOperation.ArithmeticArray(36.0d, 40.0d, 0.01d)
    val lon = ArrayOperation.ArithmeticArray(114.0d, 120.0d, 0.01d)
    val station_lat=39.946d
    val station_lon=116.328d
    val latIndex = ArrayOperation.nearIndex_sequence(lat, station_lat)
    val lonIndex = ArrayOperation.nearIndex_sequence(lon, station_lon)
    println("station_lat="+station_lat+",station_lon="+station_lon+";latIndex="+latIndex+",lonIndex="+lonIndex)
  }
}
