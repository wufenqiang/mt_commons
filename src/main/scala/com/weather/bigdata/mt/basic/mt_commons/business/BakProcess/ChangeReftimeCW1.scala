package com.weather.bigdata.mt.basic.mt_commons.business.BakProcess

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
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

private object ChangeReftimeCW1 {
  val ChangeReftimeKey: String = Constant.ChangeReftimeKey
  private val checkStationOpen = PropertiesUtil.checkStationOpen

  //目标时间已存在完整数据,开启覆盖式操作

  private val dateOp = new DateOperation

  def changereftime(jsonFile: String, fcTypes: Array[String], fcdatenew: Date, timeSteps: Array[Double], fcdateold: Date, stationsInfos: (mutable.HashMap[String, String], String, Date), changeRFCover: Boolean, changeRFCoverAll:Boolean): Boolean = {
    val sc=ContextUtil.getSparkContext()
    val appId=ContextUtil.getApplicationID

    val fcdateoldStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdateold)
    val fcdatenewStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdatenew)

    val flag:Boolean={
      if (fcdatenew.equals(fcdateold)) {
        val msg = "源日期" + fcdateoldStr + "=目标日期" + fcdatenewStr
        println(msg)
        false
      } else {
        val jsonArr:Array[JSONObject]=ShareData.jsonArr_ExistOrCreat(jsonFile)
        println("jsonArr.len="+jsonArr.length+";jsonFile="+jsonFile)
        val flagArr0:Array[(String,Double,String)]=fcTypes.map(fcType=>{
          timeSteps.map(timeStep=>{
            val flag0:Boolean=ReadNcCW.ReadFcComplete(jsonArr,timeStep,fcType,fcdateold)
            if(!flag0){
              val msg0 = "changeReftime:" + fcType + ",timeStep=" + timeStep + ";源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败;源时间数据不完整,请查明原因"
              val e:Exception=new Exception(msg0)
              e.printStackTrace()
            }
            jsonArr.map(json=>{
              val idName = Analysis.analysisJsonIdName(json)
              (idName,timeStep,fcType)
            })
          }).reduce((x,y)=>x.union(y))
        }).reduce((x,y)=>x.union(y)).filter(p=>{
          val idName=p._1
          val timeStep=p._2
          val fcType=p._3
          val flag1:Boolean=ReadNcCW.ReadFcExist (idName, timeStep, fcType, fcdateold)
          flag1
        })
        println("flagArr0.len="+flagArr0.length)
        val flag0:Boolean={
          if(flagArr0.isEmpty){
            val msg ="源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败;源时间数据不存在。"
            val e: Exception = new Exception(msg)
            e.printStackTrace()
            false
          }else{
            if(!changeRFCoverAll){
              val flagRdd0=sc.makeRDD(flagArr0,flagArr0.length)
              val flag1:Boolean=flagRdd0.map(f=>{
                val idName=f._1
                val timeStep=f._2
                val fcType=f._3
                val scidataold=ReadNcCW.ReadFcScidata(idName,timeStep,fcType,fcdateold)
                val flag2: Boolean = this.changeRF(fcdateold: Date, fcdatenew: Date, scidataold: SciDatasetCW, stationsInfos: (mutable.HashMap[String, String], String, Date), appId: String, changeRFCover: Boolean)
                flag2
              }).reduce((x,y)=>(x && y))
              if(!flag1){
                val msg ="源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败。原因未知0"
                val e:Exception=new Exception(msg)
                e.printStackTrace()
              }
              flag1
            }else{
              val flagArr1:Array[(String,Double,String)]=flagArr0.filter(p=>{
                val idName=p._1
                val timeStep=p._2
                val fcType=p._3
                val flag1:Boolean=ReadNcCW.ReadFcExist (idName, timeStep, fcType, fcdatenew)
                !flag1
              })
              println("flagArr1.len="+flagArr1.length)
              val flag1:Boolean={
                if(flagArr1.isEmpty){
                  val msg ="源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败;目标时间数据完整,不需要再次计算。"
                  println(msg)
                  true
                }else{
                  val flagRdd1=sc.makeRDD(flagArr1,flagArr1.length)
                  val flag2:Boolean=flagRdd1.map(f=>{
                    val idName=f._1
                    val timeStep=f._2
                    val fcType=f._3
                    val scidataold=ReadNcCW.ReadFcScidata(idName,timeStep,fcType,fcdateold)
                    val flag3: Boolean = this.changeRF(fcdateold: Date, fcdatenew: Date, scidataold: SciDatasetCW, stationsInfos: (mutable.HashMap[String, String], String, Date), appId: String, changeRFCover: Boolean)
                    flag3
                  }).reduce((x,y)=>(x && y))
                  if(!flag2){
                    val msg ="源时间:" + fcdateoldStr + "=>目标时间:" + fcdatenewStr + ",失败。原因未知1"
                    val e:Exception=new Exception(msg)
                    e.printStackTrace()
                  }
                  flag2
                }
              }
              flag1
            }
          }
        }
        flag0
      }
    }
    flag
  }

  private def changeRF(fcdateold: Date, fcdatenew: Date, rddold: RDD[SciDatasetCW], stationsInfos: (mutable.HashMap[String, String], String, Date), changeRFCover: Boolean): Boolean = {
    val appID = ContextUtil.getApplicationID

    //设置抽站信息为广播类型
    val sc = ContextUtil.getSparkContext()
    val stationsInfos_BC: Broadcast[(mutable.HashMap[String, String], String, Date)] = sc.broadcast[(mutable.HashMap[String, String], String, Date)](stationsInfos)

    rddold.map(scidataold => this.changeRF(fcdateold, fcdatenew, scidataold, stationsInfos_BC.value, appID, changeRFCover: Boolean)).reduce((x, y) => (x && y))
  }

  private def changeRF(fcdateold: Date, fcdatenew: Date, scidataold: SciDatasetCW, stationsInfos: (mutable.HashMap[String, String], String, Date), appId: String, changeRFCover: Boolean): Boolean = {

    val start = System.currentTimeMillis()

    val hours: Double = this.dateOp.dHours(fcdateold, fcdatenew)
    val timeStep = AttributesOperationCW.gettimeStep(scidataold)
    val fcType = AttributesOperationCW.getFcType(scidataold)

    val fcdateoldStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdateold)
    val fcdatenewStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdatenew)
    val para: String = "appId=" + appId + "fcType=" + fcType + ";fcdatenew=" + fcdatenewStr + ";timeStep=" + timeStep + ";fcdateold=" + fcdateoldStr + ";changeRFCover=" + changeRFCover

    val flag ={
      if(!changeRFCover){
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
        WriteNcCW.WriteFcFile_addId_tmp(scidata, appId)
      }else{
        println("changeRFCover="+changeRFCover)
        changeRFCover
      }
    }


    val end = System.currentTimeMillis()

    WeatherShowtime.showDateStrOut1("ChangeReftime", start, end, para, "....................")

    flag
  }

}
