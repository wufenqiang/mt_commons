package com.weather.bigdata.mt.basic.mt_commons.commons.StreamUtil

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1, HDFSReadWriteUtil}
import com.weather.bigdata.it.utils.operation.JsonOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}

import scala.collection.mutable

object MapStream {
  def updateMap(jsonMsgStr:String,jsonFile:String): Unit ={
    val jsonMsg=JsonOperation.Str2JSONObject(jsonMsgStr)
    this.updateMap(jsonMsg,jsonFile)
  }
  def updateMap(jsonMsg:JSONObject,jsonFile:String): Unit ={
    val (dataType:String,applicationTime:Date,generationFileName:String,timeStamp:Date,attribute:String)=JsonStream.analysisJsonStream(jsonMsg)

    //判断作用起报时间
    val fcdate :Date= {
      if (dataType.startsWith(Constant.Gr2bKey)) {
        applicationTime
      } else if (dataType.equals(Constant.ObsKey) || dataType.equals(Constant.InputKVKey)) {
        WeatherDate.ObsGetFc(applicationTime)
      } else if (dataType.startsWith(Constant.OcfKey)) {
        WeatherDate.OcfGetFc(applicationTime)
      } else if (dataType.equals(Constant.ChangeReftimeKey)) {
        applicationTime
      } else {
        val msg="未配置数据类型对应的fcdate"
        val e:Exception=new Exception(msg)
        e.printStackTrace()
        //          new Date
        null
      }
    }

    //信号收集池
    val typeSetfileName: String = PropertiesUtil.gettypeSetfileName(fcdate,jsonFile)

    //更新数据信号收集池
    val TypeMap = MapStream.readMap(typeSetfileName)
    TypeMap.put((dataType, applicationTime), (generationFileName, timeStamp, attribute))
    MapStream.writeMap(typeSetfileName,TypeMap)
  }

  def writeMap(fileName:String, typemaps:mutable.HashMap[(String,Date),(String,Date,String)]): Boolean ={
    val conentset:Array[(String,Date)]=typemaps.keySet.toArray
    val contents:Array[String]=conentset.map(
      c=>{
        //        val timeStampStr=DataFormatUtil.YYYYMMDDHHMMSSStr1(new Date)
        val typeset0=typemaps.get(c).get
        val dataType=c._1
        val ApplicationTime=c._2
        val GenerationFileName=typeset0._1
        val timeStamp=typeset0._2
        val attribute=typeset0._3
        JsonStream.getJsonStream(dataType,ApplicationTime,GenerationFileName,timeStamp,attribute)
      }
    )
    HDFSReadWriteUtil.writeTXT(fileName,contents,false)
  }

  def readMap(fileName:String):mutable.HashMap[(String,Date),(String,Date,String)]={
    val map0: mutable.HashMap[(String, Date), (String, Date, String)] = {
      if (!HDFSOperation1.exists(fileName)) {
        println("TypeMap不存在,返回空Map,TypeMapFile=" + fileName)
        mutable.HashMap[(String, Date), (String, Date, String)]( )
      } else {
        if (HDFSFile.filesizeB(fileName) > 1) {
          val ArrStr: Array[String] = HDFSReadWriteUtil.readTXT(fileName)

          ArrStr.map(
            str => {
              val input: (String, Date, String, Date, String) = JsonStream.analysisJsonStream(str)
              val typemaps: mutable.HashMap[(String, Date), (String, Date, String)] = mutable.HashMap[(String, Date), (String, Date, String)]( )
              typemaps.put((input._1, input._2), (input._3, input._4, input._5))
              typemaps
            }
          ).reduce((x, y) => (x.++(y)))
        } else {
          val msg = "TypeMapFile文件为空,存在读写异常返回空Map"
          val e: Exception = new Exception(msg)
          e.printStackTrace( )
          new mutable.HashMap[(String, Date), (String, Date, String)]
        }
      }
    }

    println("TypeMapFile=" + fileName)
    if (!map0.isEmpty) {
      map0.foreach(f => {
        println(f)
      })
    }

    map0
  }
  def readMap_local(fileName:String):mutable.HashMap[(String,Date),(String,Date,String)] = {
    if (!HDFSOperation1.exists(fileName)) {
      println("TypeMap不存在,返回空Map,TypeMapFile=" + fileName)
      mutable.HashMap[(String, Date), (String, Date, String)]( )
    } else {
      if (HDFSFile.filesizeB(fileName) > 1) {
        val RddStr: Array[String] = HDFSReadWriteUtil.readTXT(fileName)
        RddStr.map(
          str => {
            val input: (String, Date, String, Date, String) = JsonStream.analysisJsonStream(str)
            val typemaps: mutable.HashMap[(String, Date), (String, Date, String)] = mutable.HashMap[(String, Date), (String, Date, String)]( )
            typemaps.put((input._1, input._2), (input._3, input._4, input._5))
            typemaps
          }
        ).reduce((x, y) => (x.++(y)))
      } else {
        val msg = "TypeMapFile文件为空,存在读写异常返回空Map"
        val e: Exception = new Exception(msg)
        e.printStackTrace( )
        new mutable.HashMap[(String, Date), (String, Date, String)]
      }
    }
  }

  def putMap(TypeMap:mutable.HashMap[(String,Date),(String,Date,String)],jsonMsg:String): mutable.HashMap[(String,Date),(String,Date,String)] ={
    val (dataType:String,applicationTime:Date,generationFileName:String,timeStamp:Date,attribute:String)=JsonStream.analysisJsonStream(jsonMsg)
    TypeMap.put((dataType, applicationTime), (generationFileName, timeStamp, attribute))
    TypeMap
  }

  def getLastestDataType(TypeMap:mutable.HashMap[(String,Date),(String,Date,String)],dataType:String,fcdate:Date): ((String,Date),(String,Date,String)) ={
    val theDateSet:mutable.TreeSet[Date]={
      if (dataType.equals(Constant.ObsKey)) {
        WeatherDate.FcGetObs(fcdate)
      } else if (dataType.startsWith(Constant.OcfKey)) {
        WeatherDate.FcGetOcf(fcdate)
      }else{
        val msg="没有配置dataType="+dataType+"对应的DateSet"
        val e:Exception=new Exception(msg)
        e.printStackTrace()
        new mutable.TreeSet[Date]
      }
    }
    val TypeMap0=TypeMap.filter(f=>(theDateSet.contains(f._1._2))).filter(f=>f._1._1.equals(dataType))
    if(TypeMap0.isEmpty){
      val msg = "TypeMap中没有对应的数据:dataType=" + dataType + ";对应的fcdate=" + DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
      val e:Exception=new Exception(msg)
      e.printStackTrace()
      null
    }else{
      TypeMap0.maxBy(f=>f._1._2)
    }
  }

  def sortTypeMap(TypeMap:mutable.HashMap[(String,Date),(String,Date,String)]):(mutable.HashMap[(String,Date),(String,Date,String)],mutable.HashMap[(String,Date),(String,Date,String)],mutable.HashMap[(String,Date),(String,Date,String)],mutable.HashMap[(String,Date),(String,Date,String)],mutable.HashMap[(String,Date),(String,Date,String)])={
    val typeMap_Gr2b: mutable.HashMap[(String, Date), (String, Date, String)] = TypeMap.filter(f => f._1._1.startsWith(Constant.Gr2bKey))
    val typeMap_Ocf1h: mutable.HashMap[(String, Date), (String, Date, String)] = TypeMap.filter(f => f._1._1.equals(Constant.Ocf1hKey))
    val typeMap_Ocf12h: mutable.HashMap[(String, Date), (String, Date, String)] = TypeMap.filter(f => f._1._1.equals(Constant.Ocf12hKey))
    val typeMap_Obs: mutable.HashMap[(String, Date), (String, Date, String)] = TypeMap.filter(f => f._1._1.equals(Constant.ObsKey))
    val typeMap_other:mutable.HashMap[(String,Date),(String,Date,String)]=TypeMap.filter(f=> ((!typeMap_Gr2b.contains(f._1)) && (!typeMap_Ocf1h.contains(f._1)) && (!typeMap_Ocf12h.contains(f._1)) && (!typeMap_Obs.contains(f._1)) ))
    (typeMap_Gr2b,typeMap_Ocf1h,typeMap_Ocf12h,typeMap_Obs,typeMap_other)
  }
  def getLastest(TypeMap:mutable.HashMap[(String,Date),(String,Date,String)]): ((String,Date),(String,Date,String)) ={
    if(TypeMap.isEmpty){
      println("TypeMap is Empty.")
      null
    }else{
      TypeMap.maxBy(f=>f._1._2)
    }
  }


  def main(args:Array[String]): Unit ={
    val fileName1="/D:/Data/forecast/Input/Stream_TypeSet/20180717.txt/"
    val fcdate = DateFormatUtil.YYYYMMDDHHMMSS0("20180717080000")
    val TypeMap=this.readMap(fileName1)
    val theLatest=this.getLastestDataType(TypeMap,"Ocf_12h",fcdate)
    val dataType=theLatest._1._1
    val ApplicationTime=theLatest._1._2
    val GenerationFileName=theLatest._2._1
    val timeStamp=theLatest._2._2
    val attributeJson=theLatest._2._3
    println("ApplicationTime=" + DateFormatUtil.YYYYMMDDHHMMSSStr0(ApplicationTime))
  }
}
