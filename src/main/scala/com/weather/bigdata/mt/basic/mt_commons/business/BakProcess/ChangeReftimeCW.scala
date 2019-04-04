package com.weather.bigdata.mt.basic.mt_commons.business.BakProcess

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation.DateOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Move.DataMoveCW
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object ChangeReftimeCW {
  val ChangeReftimeKey: String = Constant.ChangeReftimeKey
  private val checkStationOpen = PropertiesUtil.checkStationOpen

  //目标时间已存在完整数据,开启覆盖式操作

  private val dateOp = new DateOperation

  //  private val wd = new WeatherDate

  //  def main(args: Array[String]): Unit = {
  //    val fcTypes = Constant.FcTypes
  //    val timeSteps = Constant.TimeSteps
  //
  //
  //    /*val (fcdateold:Date,fcdatenew:Date,jsonFile:String)= {
  //      if(args.length==1){
  //        val fcdatenewStr=args(0)
  //        val fcdatenew=DataFormatUtil.YYYYMMDDHHMMSS0(fcdatenewStr)
  //        val fcdateold=wd.previousFCDate(fcdatenew)
  //        val jsonFile=PropertiesUtil.getjsonFile()
  //        (fcdateold,fcdatenew,jsonFile)
  //      }else if(args.length==2){
  //        val fcdateoldStr=args(0)
  //        val fcdatenewStr=args(1)
  //        val jsonFile=PropertiesUtil.getjsonFile()
  //        val fcdateold:Date=DataFormatUtil.YYYYMMDDHHMMSS0(fcdateoldStr)
  //        val fcdatenew:Date=DataFormatUtil.YYYYMMDDHHMMSS0(fcdatenewStr)
  //        (fcdateold,fcdatenew,jsonFile)
  //      }else{
  //        val now =new Date()
  //        val fcdatenew=wd.nextFCDate(now)
  //        val fcdateold=wd.previousFCDate(now)
  //        (fcdateold,fcdatenew)
  //      }
  //    }*/
  //    val (fcdateold: Date, fcdatenew: Date, jsonFile: String) = {
  //      val jsonFile = args(0)
  //      val now = new Date()
  //      val fcdatenew = wd.nextFCDate(now)
  //      val fcdateold = wd.previousFCDate(now)
  //      (fcdateold, fcdatenew, jsonFile)
  //    }
  //
  //
  //    // /站点数据
  //    val StnIdGetLatLon: mutable.HashMap[String, String] = {
  //      if (checkStationOpen) {
  //        ReadFile.ReadStationInfoMap_IdGetlatlon_stainfo_nat
  //      } else {
  //        println("checkStationOpen关闭,checkStationOpen=" + checkStationOpen)
  //        null
  //      }
  //    }
  //    val stationchangeReftime = PropertiesUtil.stationchangeReftime
  //    val stationInfos = (StnIdGetLatLon, stationchangeReftime, fcdatenew)
  //
  //
  //    //    val jsonFile=PropertiesUtil.getjsonFile()
  //    //    val jsonRdd=PropertiesUtil.getJsonRdd(jsonFile)
  //    this.changereftime(jsonFile, fcTypes, fcdatenew, timeSteps, fcdateold, stationInfos)
  //  }

  def changereftime(jsonFile: String, fcTypes: Array[String], fcdatenew: Date, timeSteps: Array[Double], fcdateold: Date, stationsInfos: (mutable.HashMap[String, String], String, Date), changeRFCover: Boolean, changeRFCoverAll: Boolean): Unit = {
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)

    val fcdateoldStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdateold)
    val fcdatenewStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdatenew)

    if (fcdatenew.equals(fcdateold)) {
      val msg = "源日期" + fcdateoldStr + "=目标日期" + fcdatenewStr
      println(msg)
    } else {

      val flags: Array[(Boolean, String, Double)] = fcTypes.map(
        fcType => {
          timeSteps.map(
            timeStep => {
              val fcPathold = PropertiesUtil.getfcPath(timeStep, fcType, fcdateold)
              val fcPathnew = PropertiesUtil.getfcPath(timeStep, fcType, fcdatenew)
              val fcPathold_num: Int = HDFSOperation1.listChildrenAbsoluteFile(fcPathold).size

              val flag1: (Boolean, String, Double) = {
                if (fcPathold_num != 0) {
                  val fcPathold_numTrue: Boolean = ReadNcCW.ReadFcComplete(jsonRdd, timeStep, fcType, fcdateold)
                  if (!fcPathold_numTrue) {
                    val msg = "changeReftime:" + fcType + ",timeStep=" + timeStep + ";源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败;源时间数据不完整:" + fcPathold
                    val e: Exception = new Exception(msg)
                    e.printStackTrace()
                    throw e
                    println(msg)
                    (false, fcType, timeStep)
                  } else {
                    val fcPathnew_true: Boolean = ReadNcCW.ReadFcComplete(jsonRdd, timeStep, fcType, fcdatenew)
                    val flag2 = {
                      if (fcPathnew_true) {
                        if (changeRFCover) {
                          val msg = "执行,目标时间已存在完整数据,开启覆盖式操作;fcType=" + fcType + ",timeStep=" + timeStep + ";源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ";changeRFCover=" + changeRFCover
                          println(msg)
                          true
                        } else {
                          val msg = "失败,目标时间已存在完整数据;fcType=" + fcType + ",timeStep=" + timeStep + ";源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ";changeRFCover=" + changeRFCover
                          println(msg)
                          false
                        }
                      } else {
                        val msg = "执行,目标时间数据不存在,开始复制;fcType=" + fcType + ",timeStep=" + timeStep + ";源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr
                        println(msg)
                        true
                      }
                    }
                    (flag2, fcType, timeStep)
                  }
                } else {
                  val msg = "失败,源时间没有数据;fcType=" + fcType + ",timeStep=" + timeStep + "源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ";" + fcPathold
                  println(msg)
                  val e: Exception = new Exception(msg)
                  e.printStackTrace()
                  (false, fcType, timeStep)
                }
              }
              flag1
            }
          )
        }
      ).reduce((x, y) => x.union(y)).filter(f => f._1)

      if (!flags.isEmpty) {

        val rddold: RDD[SciDatasetCW] = flags.map(f => {
          jsonRdd.map(json => {
            val idName = Analysis.analysisJsonIdName(json)
            val fcType = f._2
            val timeStep = f._3
            val scidata = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdateold)
            scidata
          })
        }).reduce((x, y) => x.union(y))

        if (!rddold.isEmpty()) {
          val flag0: Boolean = this.changeRF(fcdateold, fcdatenew, rddold, stationsInfos, changeRFCover: Boolean, changeRFCoverAll: Boolean)
        } else {
          val msg = "数据已经全部备份完毕."
          println(msg)
        }
      } else {
        val msg = "数据已经全部备份完毕."
        println(msg)
      }
    }
  }

  private def changeRF(fcdateold: Date, fcdatenew: Date, rddold: RDD[SciDatasetCW], stationsInfos: (mutable.HashMap[String, String], String, Date), changeRFCover: Boolean, changeRFCoverAll: Boolean): Boolean = {
    val appID = ContextUtil.getApplicationID

    //设置抽站信息为广播类型
    val sc = ContextUtil.getSparkContext()
    val stationsInfos_BC: Broadcast[(mutable.HashMap[String, String], String, Date)] = sc.broadcast[(mutable.HashMap[String, String], String, Date)](stationsInfos)

    rddold.map(scidataold => this.changeRF(fcdateold, fcdatenew, scidataold, stationsInfos_BC.value, appID, changeRFCover: Boolean, changeRFCoverAll: Boolean)).reduce((x, y) => (x && y))
  }

  private def changeRF(fcdateold: Date, fcdatenew: Date, scidataold: SciDatasetCW, stationsInfos: (mutable.HashMap[String, String], String, Date), appId: String, changeRFCover: Boolean, changeRFCoverAll: Boolean): Boolean = {

    val start = System.currentTimeMillis()

    val hours: Double = this.dateOp.dHours(fcdateold, fcdatenew)
    val timeStep = AttributesOperationCW.gettimeStep(scidataold)
    val fcType = AttributesOperationCW.getFcType(scidataold)

    val fcdateoldStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdateold)
    val fcdatenewStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdatenew)
    val para: String = "appId=" + appId + "fcType=" + fcType + ";fcdatenew=" + fcdatenewStr + ";timeStep=" + timeStep + ";fcdateold=" + fcdateoldStr + ";changeRFCover=" + changeRFCover

    val scidata: SciDatasetCW = DataMoveCW.datamove(scidataold, timeStep, hours)

    if (checkStationOpen) {
      val fcdate = AttributesOperationCW.getfcdate(scidata)
      val stationsInfo: mutable.HashMap[String, String] = stationsInfos._1
      val stationocf: String = stationsInfos._2
      val ocfdate = stationsInfos._3
      CheckStationCW.checkStation(appId, stationsInfo, stationocf, scidata, ocfdate, fcdate)
    } else {
      println("CheckStation开关关闭:" + checkStationOpen)
    }


    val flag = WriteNcCW.WriteFcFile_addId_tmp(scidata, appId)

    val end = System.currentTimeMillis()

    WeatherShowtime.showDateStrOut1("ChangeReftime", start, end, para, "....................")

    flag
  }

  //  private def changereftime(jsonFile: String, fcType: String, fcdatenew: Date, timeStep: Double, fcdateold: Date, stationsInfos: (mutable.HashMap[String, String], String, Date), changeRFCover: Boolean, changeRFCoverAll: Boolean): Unit = {
  //    this.changereftime(jsonFile, Array(fcType), fcdatenew, Array(timeStep), fcdateold, stationsInfos,changeRFCover, changeRFCoverAll)
  //  }
}
