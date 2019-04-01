package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.latlonUtil

object latlonIndexGetlatlon {
  def main(args:Array[String]): Unit ={
    val latlonStep=0.03d
    val s="3492_1188"
    val latIndex=s.split("_").head.toInt
    val lonIndex=s.split("_").last.toInt
    val lat:Double=latIndex*latlonStep
    val lon:Double=lonIndex*latlonStep+70
    println("lat="+lat+"&lon="+lon)
  }
}
