package com.weather.bigdata.mt.basic.mt_commons.business.Ocf

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.ShowUtil
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object OcfReviseCW {
  private val checkStationOpen:Boolean=PropertiesUtil.checkStationOpen

  def ReturnFlag (jsonFile: String, timeStamp: Date, timeSteps: Array[Double], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date), fcType: String): Boolean = {
    this.ReturnFlag(jsonFile, timeStamp, timeSteps, sciGeo, ocffileArr, stationsInfos, Array(fcType))
  }

  //fc流程及ocf订正所走的接口
  def ReturnFlag (jsonFile: String, timeStamp: Date, timeSteps: Array[Double], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date), fcTypes: Array[String]): Boolean = {
    println("---------------------------jsonFile=" + jsonFile + ";timeStamp=" + timeStamp + ";timeSteps=" + ShowUtil.ArrayShowStr(timeSteps) + ";sciGeo.name=" + sciGeo.datasetName + ";stationsInfos=" + stationsInfos + ";fcTypes=" + ShowUtil.ArrayShowStr(fcTypes))
    //使用信号时间戳计算入库对应的时间
    val fcdate = WeatherDate.ObsGetFc(timeStamp)
    val rdds: RDD[SciDatasetCW] = ReadNcCW.ReadFcRdd(jsonFile, timeSteps, fcTypes, fcdate)
    OcfReviseCW1.ReturnFlag(rdds, sciGeo, ocffileArr, stationsInfos)
  }

  def ReturnData (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)]): RDD[SciDatasetCW] = {
    OcfReviseCW1.ReturnData(rdds, sciGeo, ocffileArr)
  }

  def ReturnData_plusStation (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] = {
    OcfReviseCW1.ReturnData_plusStation(rdds, sciGeo, ocffileArr, stationsInfos)
  }

  def ReturnFlag (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    OcfReviseCW1.ReturnFlag(rdds, sciGeo, ocffileArr, stationsInfos)
  }

  def main (args: Array[String]): Unit = {

    val ocfdate: Date = DateFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
    val timeSteps = Array(1.0d,12.0)
    val fcTypes = Array(Constant.WEATHER,Constant.PRE,Constant.TEM)


    val ocffileArr: Array[(Double, Date, String)] = timeSteps.map(timeStep => {


      val ocffile0 = PropertiesUtil.getocffile(ocfdate, timeStep)

      (timeStep, ocfdate, ocffile0)
    })

    val jsonFile = PropertiesUtil.getjsonFile( )
    val jsonRdd: RDD[JSONObject] = ShareData.jsonRdd_ExistOrCreat(jsonFile)

    //    val ocfMap:Map[String, Map[String,Map[String,Double]]] = ReadFile.ReadOcfFile(ocfPath,timeStep,ocfdate,appId)
    //读取行政参数文件
    val geoFile = PropertiesUtil.getGeoFile( )
    val sciGeo = ShareData.sciGeoCW_ExistOrCreat
    // /站点数据
    val stationInfo: mutable.HashMap[String, String] = {
      if (checkStationOpen) {
        ReadFile.ReadStationInfoMap_IdGetlatlon0( )
      } else {
        println("checkStationOpen关闭,checkStationOpen=" + checkStationOpen)
        null
      }
    }
    val stationocf = PropertiesUtil.stationocf
    val stationInfos = (stationInfo, stationocf, ocfdate)

    val timeStamp = DateFormatUtil.YYYYMMDDHHMMSS0("20180308100000")
    val flag = this.ReturnFlag(jsonFile, timeStamp, timeSteps, sciGeo, ocffileArr, stationInfos, fcTypes)

    println("flag=" + flag)
  }
}
