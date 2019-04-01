package com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFile

import java.text.SimpleDateFormat
import java.util.Date

import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil

class WeatherToFileName {
  def GetNcFileName (sourceType: String, FcType: String, t0: Float, t1: Float, h0: Float, h1: Float, LatLon: String, timeStep: Float, heighStep: Float, LatLonStep: Float, scale_factor: Int, date: Date): String = { //"nmc_"+(int)timeStep+"h_CLOUD_"+String.valueOf((int)times2[i][0])+"_0_0_70_1_0_0.01_0.01_"+(String.valueOf((int)times2[i][times2[0].length-1]))+"_0_60_140_0_201707130800"
    val LatLonSplit = LatLon.split(",")
    val startLat = LatLonSplit(0).toFloat
    val endLat = LatLonSplit(1).toFloat
    val startLon = LatLonSplit(2).toFloat
    val endLon = LatLonSplit(3).toFloat
    val sdfh = new SimpleDateFormat("yyyyMMddHHmm")
    val sb = new StringBuffer
    sb.append(sourceType).append("_").append(String.valueOf(timeStep.toInt) + "h").append("_").append(FcType).append("_").append(t0.toInt).append("_").append(h0.toInt).append("_").append(startLat.toInt).append("_").append(startLon.toInt).append("_").append(timeStep.toInt).append("_").append(heighStep.toInt).append("_").append(LatLonStep).append("_").append(LatLonStep).append("_").append(t1.toInt).append("_").append(h1.toInt).append("_").append(endLat.toInt).append("_").append(endLon.toInt).append("_").append(scale_factor).append("_").append(sdfh.format(date)).append(".nc")
    sb.toString
  }

}

object WeatherToFileName {
  private val wtfn = new WeatherToFileName

  def main (args: Array[String]): Unit = {
    val df = new DateFormatUtil
    val sourceType = "nmc"
    val FcType = "PRE"
    val t0 = -1f
    val t1 = -1f
    val h0 = -1f
    val h1 = -1f
    val LatLon = "0,60,70,140"
    val timeStep = 1.0f
    val heightStep = 1.0f
    val LatLonStep = 0.01f
    val scale_factor = 1
    val fcdate = df.YYYYMMDDHHMMSS0("20190219080000")
    System.out.println(this.GetNcFileName(sourceType, FcType, t0, t1, h0, h1, LatLon, timeStep, heightStep, LatLonStep, scale_factor, fcdate))
  }

  def GetNcFileName (sourceType: String, FcType: String, t0: Float, t1: Float, h0: Float, h1: Float, LatLon: String, timeStep: Float, heighStep: Float, LatLonStep: Float, scale_factor: Int, date: Date): String = {
    this.wtfn.GetNcFileName(sourceType, FcType, t0, t1, h0, h1, LatLon, timeStep, heighStep, LatLonStep, scale_factor, date)
  }
}