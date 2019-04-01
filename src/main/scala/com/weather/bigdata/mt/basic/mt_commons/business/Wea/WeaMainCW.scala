package com.weather.bigdata.mt.basic.mt_commons.business.Wea

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.mt.basic.mt_commons.business.Ocf.OcfReviseCW
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object WeaMainCW {

  private val pphWea3Open: Boolean = false

  def afterFcWea0 (jsonFile: String, fcType: String, timeSteps: Array[Double], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val rdd = ReadNcCW.ReadFcRdd(jsonFile, timeSteps, fcType, fcdate)
    WeaCW1.afterFcWea0_ReturnFlag(rdd, fcdate, stationInfos)
  }

  def afterFcWea0_ReturnFlag(rdd: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)):Boolean={
    WeaCW1.afterFcWea0_ReturnFlag(rdd, fcdate, stationInfos)
  }
  def afterFcWea0_ReturnRdd(rdd: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] ={
    WeaCW1.afterFcWea0_ReturnRdd_plusStation(rdd, fcdate, stationInfos)
  }

  def afterFcOcfWea2 (jsonFile: String, timeStamp: Date, fcType: String, timeSteps: Array[Double], fcdate: Date, sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val rdd0: RDD[SciDatasetCW] = ReadNcCW.ReadFcRdd(jsonFile, timeSteps, fcType, fcdate)
    val rdd1: RDD[SciDatasetCW] = WeaCW1.afterFcWea0_ReturnRdd(rdd0, fcdate)
    //    val ocffileArr=ocffileArr0.filter(p=>p._3.equals(Constant.WEATHER))
    OcfReviseCW.ReturnFlag(rdd1, sciGeo, ocffileArr, stationInfos)
  }
  def afterFcOcfWea2 (rdd:RDD[SciDatasetCW],sciGeo:SciDatasetCW,ocffileArr: Array[(Double, Date, String)],stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean ={
    OcfReviseCW.ReturnFlag(rdd, sciGeo, ocffileArr, stationInfos)
  }

  def afterObsWea1 (jsonFile: String, obsdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val fcdate = WeatherDate.ObsGetFc(obsdate)
    WeaCW1.afterObsWea1(jsonFile, fcdate, timeSteps, stationInfos)
  }

  def pphWea3 (jsonFile: String, pphfileName: String, fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    if (this.pphWea3Open) {
      WeaCW1.pphWea3(jsonFile, pphfileName, fcdate, timeSteps, stationInfos)
    } else {
      val e: Exception = new Exception("pphWea3关闭")
      e.printStackTrace( )
      false
    }
  }


  def afterOcfWea2 (jsonFile: String, timeStamp: Date, timeSteps: Array[Double], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    OcfReviseCW.ReturnFlag(jsonFile, timeStamp, timeSteps, sciGeo, ocffileArr, stationsInfos, Constant.WEATHER)
  }

  def main (args: Array[String]): Unit = {

    val ocfdate: Date = DateFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
    val timeSteps = Array(1.0d,12.0d)

    val ocffileArr: Array[(Double, Date, String)] = timeSteps.map(timeStep => {


      val ocffile0 = PropertiesUtil.getocffile(ocfdate, timeStep)

      (timeStep, ocfdate, ocffile0)
    })

    val jsonFile = PropertiesUtil.getjsonFile( )
    val jsonRdd: RDD[JSONObject] = ShareData.jsonRdd_ExistOrCreat(jsonFile)

    //    val ocfMap:Map[String, Map[String,Map[String,Double]]] = ReadFile.ReadOcfFile(ocfPath,timeStep,ocfdate,appId)
    //读取行政参数文件
    val sciGeo = ShareData.sciGeoCW_ExistOrCreat
    // /站点数据
    val stationInfo: mutable.HashMap[String, String] = ReadFile.ReadStationInfoMap_IdGetlatlon0( )

    val stationocf = PropertiesUtil.stationocf
    val stationInfos = (stationInfo, stationocf, ocfdate)

    val timeStamp = DateFormatUtil.YYYYMMDDHHMMSS0("20180308100000")

    val flag = this.afterOcfWea2(jsonFile, timeStamp, timeSteps, sciGeo, ocffileArr, stationInfos)

    println("flag=" + flag)
  }
}
