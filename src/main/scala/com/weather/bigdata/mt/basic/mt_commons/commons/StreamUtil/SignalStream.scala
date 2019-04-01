package com.weather.bigdata.mt.basic.mt_commons.commons.StreamUtil

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.spark.platform.signal.{JsonStream, anaAttribute}
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}


object SignalStream {

  private val obssignalOpen:Boolean=PropertiesUtil.obssignalOpen
  private val indexsignalOpen:Boolean=PropertiesUtil.indexsignalOpen
  private val reduceLatLonsignalOpen: Boolean = PropertiesUtil.reduceLatLonsignalOpen

  private val regionKey = Constant.regionKey

  private def getIndexsignal (fcdate: Date, timeStamp: Date): String = {
    val fcdateStr = DateFormatUtil.YYYYMMDDHHStr0(fcdate)
    //    val content: String = fcdateStr + ",true"
    val dataType = "fc"
    val fcPath = PropertiesUtil.getfcPath(fcdate)
    val attribute: String = ""
    val content = JsonStream.getJsonStream(dataType, fcdate, fcPath, timeStamp, attribute)
    content
  }

  def writeIndexsignal (jsonFile: String, fcdate: Date, timeSteps: Array[Double]): Unit = {
    timeSteps.foreach(timeStep => this.writeIndexsignal(jsonFile, fcdate, timeStep))
  }

  def writeIndexsignal (jsonFile: String, fcdate: Date, timeStep: Double): Unit = {
    if (jsonFile.contains(this.regionKey)) {
      if (this.indexsignalOpen) {
        if (timeStep == 12.0d) {
          val fileName = PropertiesUtil.getIndexSignalFile(fcdate)
          //        val fcdateStr=DateFormatUtil.YYYYMMDDHHStr0(fcdate)
          //        val conent:String=fcdateStr+",true"
          val timeStamp = new Date
          val content: String = this.getIndexsignal(fcdate, timeStamp)
          HDFSReadWriteUtil.writeTXT(fileName, content, false)

          println(DateFormatUtil.YYYYMMDDHHMMSSStr1(timeStamp) + ";Index信号:" + fileName)
        }
      }
    } else {
      val msg = "jsonFile=" + jsonFile + ",不包含" + regionKey + ",不发送Indexsignal"
      println(msg)
    }
  }

  private def getObssignal (timeStep: Double, fcType: String, fcdate: Date, timeStamp: Date): String = {
    val fcPath = PropertiesUtil.getfcPath(timeStep, fcType, fcdate)
    val dataType = "fc_" + fcType
    val attribute: String = ""
    val content = JsonStream.getJsonStream(dataType, fcdate, fcPath, timeStamp, attribute)
    content
  }

  def writeObssignal (jsonFile: String, fcdate: Date, fcTypes: Array[String], timeStep: Double): Unit = {
    fcTypes.foreach(fcType => this.writeObssignal(jsonFile, fcdate, fcType, timeStep))
  }

  def writeObssignal (jsonFile: String, fcdate: Date, fcType: String, timeStep: Double): Unit = {
    if (jsonFile.contains(this.regionKey)) {
      if (this.obssignalOpen) {
        if (timeStep == 1.0d) {
          val fileName = PropertiesUtil.getObsSignalFile(fcdate, fcType)
          //        val fcPath = PropertiesUtil.getfcPath(timeStep,fcType,fcdate)
          //        val dataType="fc_"+fcType
          //        val attribute:String=""
          //        val content:String=JsonStream.getJsonStream(dataType,fcdate,fcPath,new Date(),attribute)

          val now = new Date
          val content: String = this.getObssignal(timeStep, fcType, fcdate, now)
          HDFSReadWriteUtil.writeTXT(fileName, content, false)

          val timeStamp = DateFormatUtil.YYYYMMDDHHMMSSStr1(now)
          println(timeStamp + ";Obs信号:" + fileName)
        }
      }
    } else {
      val msg = "jsonFile=" + jsonFile + ",不包含" + regionKey + ",不发送Obssignal"
      println(msg)
    }

  }

  def writeObssignal (jsonFile: String, fcdate: Date, fcTypes: Array[String], timeSteps: Array[Double]): Unit = {
    fcTypes.foreach(fcType => this.writeObssignal(jsonFile, fcdate, fcType, timeSteps))
  }

  def writeObssignal (jsonFile: String, fcdate: Date, fcType: String, timeSteps: Array[Double]): Unit = {
    timeSteps.foreach(timeStep => this.writeObssignal(jsonFile, fcdate, fcType, timeStep))
  }

  def main(args:Array[String]): Unit ={
    val Fcdate = new Date( )
    val jsonFile = "/D:/Data/forecast/Input/region0.txt"
    val fcTypes = Array("PRE", "TEM", "WIND", "WINUV", "VIS")
    val timeSteps: Array[Double] = Array(1.0d, 12.0d)
    fcTypes.foreach(fcType => {
      this.reduceLatLonsignal(jsonFile, Fcdate, fcType, timeSteps)
    })

  }

  def reduceLatLonsignal (jsonFile: String, fcdate: Date, fcType: String, timeSteps: Array[Double]): Unit = {
    if (jsonFile.contains(this.regionKey)) {
      val timeStamp = new Date
      if (fcType.equals(Constant.WIND)) {
        //风向
        val fileName0 = PropertiesUtil.getReduceLatLonSignalFile(fcdate, Constant.WIND)
        val content0: String = this.getReduceLatLonsignal(jsonFile, Constant.WIND, fcdate, timeStamp, timeSteps)
        HDFSReadWriteUtil.writeTXT(fileName0, content0, false)
        println(DateFormatUtil.YYYYMMDDHHMMSSStr1(timeStamp) + ";reduceLatLonsignal信号:" + fileName0)

        //风速
        val fileName1 = PropertiesUtil.getReduceLatLonSignalFile(fcdate, Constant.WINUV)
        val content1: String = this.getReduceLatLonsignal(jsonFile, Constant.WINUV, fcdate, timeStamp, timeSteps.filter(timeStep => timeStep != 1.0d))
        HDFSReadWriteUtil.writeTXT(fileName1, content1, false)
        println(DateFormatUtil.YYYYMMDDHHMMSSStr1(timeStamp) + ";reduceLatLonsignal信号:" + fileName1)

      } else {
        val fileName = PropertiesUtil.getReduceLatLonSignalFile(fcdate, fcType)
        val content: String = this.getReduceLatLonsignal(jsonFile, fcType, fcdate, timeStamp, timeSteps)
        HDFSReadWriteUtil.writeTXT(fileName, content, false)
        println(DateFormatUtil.YYYYMMDDHHMMSSStr1(timeStamp) + ";reduceLatLonsignal信号:" + fileName)
      }
    } else {
      val msg = "jsonFile=" + jsonFile + ",不包含" + regionKey + ",不发送reduceLatLonsignal"
      println(msg)
    }
  }

  private def getReduceLatLonsignal (jsonFile: String, fcType: String, fcdate: Date, timeStamp: Date, timeSteps: Array[Double]): String = {

    val fcdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
    val fcPath = PropertiesUtil.getfcPath(fcType, fcdate)

    val dataType: String = {
      if (fcType.equals(Constant.PRE)) {
        Constant.reduceLatLon_PREPathKey
      } else if (fcType.equals(Constant.TEM)) {
        Constant.reduceLatLon_TEMPathKey
      } else if (fcType.equals(Constant.CLOUD)) {
        Constant.reduceLatLon_CLOUDPathKey
      } else if (fcType.equals(Constant.WIND)) {
        Constant.reduceLatLon_WINDPathKey
      } else if (fcType.equals(Constant.WINUV)) {
        Constant.reduceLatLon_WINUVPathKey
      } else if (fcType.equals(Constant.RHU)) {
        Constant.reduceLatLon_RHUPathKey
      } else if (fcType.equals(Constant.PRS)) {
        Constant.reduceLatLon_PRSPathKey
      } else if (fcType.equals(Constant.VIS)) {
        Constant.reduceLatLon_VISPathKey
      } else if (fcType.equals(Constant.WEATHER)) {
        Constant.reduceLatLon_WEATHERPathKey
      } else if (fcType.equals(Constant.reduceLatLon)) {
        Constant.reduceLatLon
      } else {
        val msg: String = "fcType=" + fcType + "未配置dataType"
        val e: Exception = new Exception(msg)
        e.printStackTrace( )
        "reduce_"
      }
    }
    val attribute: String = {
      val attributeJson: JSONObject = new JSONObject( )
      anaAttribute.putTimeSteps(attributeJson, timeSteps)
      anaAttribute.putSplitFile(attributeJson, jsonFile)
      //      attributeJson.put(Constant.splitFileKey,jsonFile)
      attributeJson.toString
    }
    val content = JsonStream.getJsonStream(dataType, fcdate, fcPath, timeStamp, attribute)
    content

  }

  def reduceLatLonsignal (jsonFile: String, fcdate: Date, fcTypes: Array[String], timeSteps: Array[Double]): Unit = {
    fcTypes.foreach(fcType => this.reduceLatLonsignal(jsonFile, fcdate, fcType, timeSteps))
  }


}
