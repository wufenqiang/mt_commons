package com.weather.bigdata.mt.basic.mt_commons.business.Wea

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.CopyRddUtil.CpRddCW
import com.weather.bigdata.mt.basic.mt_commons.commons.PheUtil.PheCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 天气现象计算测试方法
  * Created by wufenqiang on 2018/03/30
  */
private object WeaCW1 {
  private val checkPointOpen: Boolean = PropertiesUtil.checkStationOpen

  def afterFcWea0_ReturnFlag (rdd0: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    this.parallel5wea0_ReturnFlag(rdd0, fcdate, stationInfos)
  }

  private def parallel5wea0_ReturnFlag (rdd0: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val appID: String = ContextUtil.getApplicationID
    val flag = rdd0.map(scidata => this.parallel5wea0_ReturnFlag(scidata, fcdate, stationInfos, appID)).reduce((x, y) => (x && y))
    flag
  }

  private def parallel5wea0_ReturnFlag (scidata: SciDatasetCW, fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date), appID: String): Boolean = {
    val wea0Scidata: SciDatasetCW = this.parallel5wea0_ReturnData_plusStation(scidata, fcdate, stationInfos, appID)
    val flag0 = WriteNcCW.WriteFcFile_addId_tmp(wea0Scidata, appID)
    flag0
  }

  private def parallel5wea0_ReturnData_plusStation (scidata: SciDatasetCW, fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date), appID: String): SciDatasetCW = {
    val wea0Scidata: SciDatasetCW = this.parallel5wea0_ReturnData(scidata: SciDatasetCW, fcdate: Date, appID: String)
    //抽站
    if (this.checkPointOpen) {
      val stationInfo = stationInfos._1
      val checkOutRootPath = stationInfos._2
      val thedate = stationInfos._3
      CheckStationCW.checkStation(appID, stationInfo, checkOutRootPath, wea0Scidata, thedate, fcdate)
    } else {
      println("CheckStation开关关闭:" + this.checkPointOpen)
    }
    wea0Scidata
  }

  private def parallel5wea0_ReturnData (scidata: SciDatasetCW, fcdate: Date, appID: String): SciDatasetCW = {
    val fcType = AttributesOperationCW.getFcType(scidata)
    val idaName = AttributesOperationCW.getIDName(scidata)
    val timeStep = AttributesOperationCW.gettimeStep(scidata)
    val PRES: SciDatasetCW = {
      if (fcType.equals(Constant.PRE)) {
        scidata
      } else {
        ReadNcCW.ReadFcScidata(idaName, timeStep, Constant.PRE, fcdate)
      }
    }
    val TEMS: SciDatasetCW = {
      if (fcType.equals(Constant.TEM)) {
        scidata
      } else {
        ReadNcCW.ReadFcScidata(idaName, timeStep, Constant.TEM, fcdate)
      }
    }
    val CLOUDS: SciDatasetCW = {
      if (fcType.equals(Constant.CLOUD)) {
        scidata
      } else {
        ReadNcCW.ReadFcScidata(idaName, timeStep, Constant.CLOUD, fcdate)
      }
    }
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val weatherName = PropertiesUtil.weatherName
    val wea0Scidata: SciDatasetCW = PheCW.Weather_Wea0(PRES, TEMS, CLOUDS, timeName, heightName, latName, lonName, timeStep, weatherName)

    wea0Scidata
  }

  def afterFcWea0_ReturnRdd (rdd0: RDD[SciDatasetCW], fcdate: Date): RDD[SciDatasetCW] = {
    this.parallel5wea0_ReturnData(rdd0, fcdate)
  }

  private def parallel5wea0_ReturnData (rdd0: RDD[SciDatasetCW], fcdate: Date): RDD[SciDatasetCW] = {
    val appID: String = ContextUtil.getApplicationID
    val rdd = rdd0.map(scidata => this.parallel5wea0_ReturnData(scidata, fcdate, appID))
    rdd
  }

  def afterFcWea0_ReturnRdd_plusStation (rdd0: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] = {
    this.parallel5wea0_ReturnData_plusStation(rdd0, fcdate, stationInfos)
  }

  private def parallel5wea0_ReturnData_plusStation (rdd0: RDD[SciDatasetCW], fcdate: Date, stationInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] = {
    val appID: String = ContextUtil.getApplicationID
    val rdd = rdd0.map(scidata => this.parallel5wea0_ReturnData_plusStation(scidata, fcdate, stationInfos, appID))
    rdd
  }

  def afterObsWea1 (jsonFile: String, fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    this.afterObsWea1(jsonRdd, fcdate, timeSteps, stationInfos)
  }

  def afterObsWea1 (jsonRdd: RDD[JSONObject], fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val flag: Boolean = this.parallelwea1(jsonRdd, fcdate, timeSteps, stationInfos)
    if (flag) {
      println("parallelwea1计算成功")
    } else {
      println("parallelwea1计算失败")
    }
    flag
  }

  private def parallelwea1 (jsonRdd: RDD[JSONObject], fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val appID = ContextUtil.getApplicationID
    val dateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
    val para = "dateStr=" + dateStr
    val Start = System.currentTimeMillis( )
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Wea.parallelwea1.Start:(" + para + ")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    val cpJsonRdds = CpRddCW.cpJsontimeSteps(jsonRdd, timeSteps)


    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val weatherName = PropertiesUtil.weatherName

    val flag: Boolean = cpJsonRdds.map(
      f => {
        val idName: String = f._1
        val timeStep: Double = f._2

        val WeaStart = System.currentTimeMillis( )

        val PRES: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.PRE, fcdate)
        val TEMS: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.TEM, fcdate)
        val CLOUDS: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.CLOUD, fcdate)
        val WEAS: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.WEATHER, fcdate)

        val WEATHER = PheCW.Weather_Wea1(PRES, TEMS, CLOUDS, WEAS, timeName, heightName, latName, lonName, timeStep, weatherName)

        //        val path_WEATHER=PropertiesUtil.getfcPath(timeStep,ConstantUtil.WEATHER,fcdate)
        val flag0 = WriteNcCW.WriteFcFile_addId_tmp(WEATHER, appID)

        val WeaEnd = System.currentTimeMillis( )
        WeatherShowtime.showDateStrOut1("Wea.parallelwea1:", WeaStart, WeaEnd)

        if (this.checkPointOpen) {
          val checkStart = System.currentTimeMillis( )
          val stationInfo = stationInfos._1
          val checkOutRootPath = stationInfos._2
          val thedate = stationInfos._3
          CheckStationCW.checkStation(appID, stationInfo, checkOutRootPath, WEATHER, thedate, fcdate)
          val checkEnd = System.currentTimeMillis( )
          WeatherShowtime.showDateStrOut1("checkStation:", checkStart, checkEnd)
        } else {
          println("CheckStation开关关闭:" + this.checkPointOpen)
        }
        flag0
      }
    ).reduce((x, y) => (x && y))


    val End = System.currentTimeMillis( )
    //    ws.showDateStrOut1("parallel4wea0过程:",Start,End)
    WeatherShowtime.showDateStrOut1("parallelwea1过程:", Start, End, para, "para")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Wea.parallelwea1.End:(" + para + ")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    flag
  }

  def pphWea3 (jsonFile: String, pphfileName: String, fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    this.pphWea3(jsonRdd, pphfileName, fcdate, timeSteps, stationInfos)
  }

  def pphWea3 (jsonRdd: RDD[JSONObject], pphfileName: String, fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {


    val varsName_pph: (String, String, String) = PropertiesUtil.getVarName_pph( )
    val pph_Sci = AttributesOperationCW.addVarName_tlatlon(ReadNcCW.ReadGr2bFile(pphfileName, fcdate, Constant.PPH), varsName_pph)


    val flag: Boolean = this.parallelwea3(jsonRdd, pph_Sci, fcdate, timeSteps, stationInfos)
    if (flag) {
      println("pphWea3计算成功")
    } else {
      println("pphWea3计算失败")
    }
    flag
  }

  private def parallelwea3 (jsonRdd: RDD[JSONObject], pph_Sci: SciDatasetCW, fcdate: Date, timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val appID = ContextUtil.getApplicationID
    val dateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
    val para = "dateStr=" + dateStr
    val Start = System.currentTimeMillis( )
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Wea.parallelwea3.Start:(" + para + ")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    val cpJsonRdds = CpRddCW.cpJsontimeSteps(jsonRdd, timeSteps)


    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val weatherName = PropertiesUtil.weatherName

    val flag: Boolean = cpJsonRdds.map(
      f => {
        val idName: String = f._1
        val timeStep: Double = f._2

        val WeaStart = System.currentTimeMillis( )

        val PRES: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.PRE, fcdate)
        val CLOUDS: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, Constant.CLOUD, fcdate)

        val WEATHER = PheCW.Weather_Wea3(PRES, CLOUDS, timeName, heightName, latName, lonName, pph_Sci, timeStep, weatherName)

        //        val path_WEATHER=PropertiesUtil.getfcPath(timeStep,ConstantUtil.WEATHER,fcdate)
        val flag0 = WriteNcCW.WriteFcFile_addId_tmp(WEATHER, appID)

        val WeaEnd = System.currentTimeMillis( )
        WeatherShowtime.showDateStrOut1("Wea.parallelwea3:", WeaStart, WeaEnd)

        if (this.checkPointOpen) {
          val checkStart = System.currentTimeMillis( )
          val stationInfo = stationInfos._1
          val checkOutRootPath = stationInfos._2
          val thedate = stationInfos._3
          CheckStationCW.checkStation(appID, stationInfo, checkOutRootPath, WEATHER, thedate, fcdate)
          val checkEnd = System.currentTimeMillis( )
          WeatherShowtime.showDateStrOut1("checkStation:", checkStart, checkEnd)
        } else {
          println("CheckStation开关关闭:" + this.checkPointOpen)
        }
        flag0
      }
    ).reduce((x, y) => (x && y))


    val End = System.currentTimeMillis( )
    //    ws.showDateStrOut1("parallel4wea0过程:",Start,End)
    WeatherShowtime.showDateStrOut1("parallelwea3过程:", Start, End, para, "para")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Wea.parallelwea3.End:(" + para + ")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    flag
  }
}
