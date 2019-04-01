package com.weather.bigdata.mt.basic.mt_commons.business.Fc

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.ShowUtil
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object FcMainCW {
  private val checkStationOpen = PropertiesUtil.checkStationOpen

  def parallelfc_ReturnRDD(jsonFile: String, fcType: String, fcdate: Date, gr2bfileName: String, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): RDD[SciDatasetCW] = {
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    this.parallelfc_ReturnRDD(jsonRdd, fcType, fcdate, gr2bfileName, ExtremMap0, stationsInfos, gr2bFrontOpen, timeSteps)
  }

  def parallelfc_ReturnRDD(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, gr2bfileName: String, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): RDD[SciDatasetCW] = {
    val unSplitScidata = ReadNcCW.ReadGr2bFile(gr2bfileName, fcdate, fcType)
    FcMainCW1.parallelfc_ReturnRDD(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata, ExtremMap0, stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double])
  }

  def parallelfc_ReturnFlag(jsonFile: String, fcType: String, fcdate: Date, gr2bfileName: String, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): Boolean = {
    println("-------------FcMain:jsonFile=" + jsonFile + ";fcType=" + fcType + ";fcdate=" + fcdate + ";gr2bfileName=" + gr2bfileName + ";ExtremMap0=" + ExtremMap0 + ";stationsInfos=" + stationsInfos + ";gr2bFrontOpen=" + gr2bFrontOpen + ";timeSteps=" + ShowUtil.ArrayShowStr(timeSteps))
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    this.parallelfc_ReturnFlag(jsonRdd, fcType, fcdate, gr2bfileName, ExtremMap0, stationsInfos, gr2bFrontOpen, timeSteps)
  }

  def parallelfc_ReturnFlag(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, gr2bfileName: String, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): Boolean = {
    val unSplitScidata: SciDatasetCW = ReadNcCW.ReadGr2bFile(gr2bfileName, fcdate, fcType)
    FcMainCW1.parallelfc_ReturnFlag(jsonRdd, fcType, fcdate, unSplitScidata, ExtremMap0, stationsInfos, gr2bFrontOpen, timeSteps)
  }

  //ObsRevise0使用
  def nmc2fcRdd(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, gr2bfileName: String, ExtremMap0: Map[String, Map[String, Array[Float]]], gr2bFrontOpen: Boolean, timeStep: Double, appID: String): RDD[SciDatasetCW] = {
    val unSplitScidata: SciDatasetCW = ReadNcCW.ReadGr2bFile(gr2bfileName, fcdate, fcType)
    FcMainCW1.nmc2fcRdd(jsonRdd, fcType, fcdate, unSplitScidata, ExtremMap0, gr2bFrontOpen, timeStep, appID)
  }

  //测试用
  def main(args: Array[String]): Unit = {
    // /站点数据
    val StnIdGetLatLon: mutable.HashMap[String, String] = {
      if (this.checkStationOpen) {
        ReadFile.ReadStationInfoMap_IdGetlatlon_stainfo_nat
      } else {
        println("checkStationOpen关闭,checkStationOpen=" + this.checkStationOpen)
        null
      }
    }
    val jsonFile = PropertiesUtil.getjsonFile()

    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)

    val fcdate = DateFormatUtil.YYYYMMDDHHMMSS0("20180308080000")

    //    val fcType="VIS"
    //    val gr2bfileName="/D:/Data/forecast/Input/Real/20180308/08/VIS/VIS.nc"

    val fcType = "TEM"
    val gr2bfileName = "/D:/Data/forecast/Input/Real/20180308/08/TMP/2018030808_TMP.nc"

    val ExtremMap = ReadFile.ReadTemExFile(PropertiesUtil.getTemExtremPath())
    val gr2bFrontOpen = false
    val stationfc = PropertiesUtil.stationfc
    val stationInfos = (StnIdGetLatLon, stationfc, fcdate)
    val timeSteps = Array(12.0d)
    val flag = this.parallelfc_ReturnFlag(jsonRdd, fcType, fcdate, gr2bfileName, ExtremMap, stationInfos, gr2bFrontOpen, timeSteps)
    println(flag)
  }
}
