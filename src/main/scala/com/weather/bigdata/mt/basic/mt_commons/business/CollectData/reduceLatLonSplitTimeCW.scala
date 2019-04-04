package com.weather.bigdata.mt.basic.mt_commons.business.CollectData

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{SciDatasetOperationCW, VariableOperationCW}
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Offset.DataOffsetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Replace.DataReplaceCW
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object reduceLatLonSplitTimeCW {
  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )

    val fcdate: Date = {
      if (PropertiesUtil.isPrd) {
        DateFormatUtil.YYYYMMDDHHMMSS0(args(0))
      } else {
        DateFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
      }
    }
    val appId = ContextUtil.getApplicationID
    val fcTypes = {
      if (PropertiesUtil.isPrd) {
        Constant.FcTypes_reduceLatLon
      } else {
        Array(Constant.CLOUD, Constant.TEM, Constant.PRE)
      }
      //      Array(ConstantUtil.WIND)
    }
    val timeSteps = {
      if (PropertiesUtil.isPrd) {
        //        ConstantUtil.TimeSteps
        Array(1.0d, 12.0d)
      } else {
        Array(1.0d, 12.0d)
      }
    }

    val heightStep = 0.0d
    val latlonStep = 0.01d

    /*val outpath_root:String={
      if(PropertiesUtil.isPrd){
//        "hdfs://dataflow-node-1:9000/data/dataSource/test"
        "hdfs://dataflow-node-1:9000/data/dataSource/fc/grid/WEATHER/output_reduceLatLon/"
      }else{
        "D:\\tmp\\"
      }
    }*/
    val jsonFile = {
      if (PropertiesUtil.isPrd) {
        args(1)
      } else {
        //        "/D:/Data/forecast/Input/JsonPRObreak77.txt/"
        PropertiesUtil.getjsonFile( )
      }
    }
    val sc = ContextUtil.getSparkContext( )
    val times: Array[(Double, Array[Double])] = timeSteps.map(timeStep => PropertiesUtil.gettimes_Split(timeStep)).reduce((x, y) => x.union(y))
    //    val timesRdd: RDD[(Double, Array[Double])] = sc.makeRDD(times, times.length)

    val fcType_times: Array[(String, Double, Array[Double])] = fcTypes.map(fcType => {
      val timeRdd0: Array[(String, Double, Array[Double])] = times.map(f => {
        val timeStep = f._1
        val times = f._2
        (fcType, timeStep, times)
      }).filter(p => {
        val fcType = p._1
        val timeStep = p._2
        !(fcType.equals(Constant.WINUV) && timeStep == 12.0d)
      })
      timeRdd0
    }).reduce((x, y) => (x.union(y)))

    val fcType_timesLen = fcType_times.length
    val fcType_timesRDD: RDD[(String, Double, Array[Double])] = sc.makeRDD(fcType_times, fcType_timesLen)

    val jsonArr = ShareData.jsonArr_ExistOrCreat(jsonFile)

    val flag = this.ReturnFlag(fcType_timesRDD, fcdate, heightStep, latlonStep, jsonArr, appId)

    println("reduceLatLonSplitTimeCW:" + flag)

    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("reduceLatLonSplitTimeCW", start, end)
  }

  /*private def reducebyLatLon2Scidata4 (fcType_timesRDD:RDD[(String,Double,Array[Double])], fcdate:Date, heightStep:Double, latlonStep:Double, jsonArr:Array[JSONObject], appId:String): RDD[SciDatasetCW] ={
    fcType_timesRDD.map(fcType_times=>this.reducebyLatLon2Scidata4(fcType_times,fcdate:Date,heightStep:Double,latlonStep:Double,jsonArr:Array[JSONObject],appId:String))
  }
  private def reducebyLatLon2Scidata4 (fcType_time:(String,Double,Array[Double]), fcdate:Date, heightStep:Double, latlonStep:Double, jsonArr:Array[JSONObject], appId:String): SciDatasetCW ={
    val fcType=fcType_time._1
    val timeStep:Double=fcType_time._2
    val times=fcType_time._3
    //    println(fcType+","+timeStep+","+ShowUtil.ArrayShowStr(times))
    val timeidName=times.min+"_("+timeStep+")_"+times.max

    val splitNum=jsonArr.length

    //    val datasetName=times.min+"_"+timeStep+"_"+times.max+"_"+fcType+"_"+splitNum+".nc"
    val (dataType:String,radio:Int)=PropertiesUtil.dataTypeFromFcType(fcType)
    val datasetName=this.dataNameFun(fcdate,fcType,radio,times.min,times.max,timeStep)

    val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
    val (height:Array[Double],lat:Array[Double],lon:Array[Double])={
      val Var=jsonArr.map(json=>{
        val (idName,timeBegin,timeEnd,timeStep,heightBegin,heightEnd,heightStep,latBegin,latEnd,latStep,lonBegin,lonEnd,lonStep)=AccordSciDatas_JsonObjectCW.analysisJsonProperties(json)
        (heightBegin,heightEnd,latBegin,latEnd,lonBegin,lonEnd)
      }).reduce((x,y)=> {
        (Math.min(x._1,y._1),Math.max(x._2,y._2),Math.min(x._3,y._3),Math.max(x._4,y._4),Math.min(x._5,y._5),Math.max(x._6,y._6))
      })



      val height0=ArrayOperation.ArithmeticArray(Var._1,Var._2,heightStep)
//      val lat0=ArrayOperation.ArithmeticArray(Var._3,Var._4,latlonStep)
//      val lon0=ArrayOperation.ArithmeticArray(Var._5,Var._6,latlonStep)

      val latlonStep0:Double=0.01d
      val (lat0,lon0)=PropertiesUtil.getLatLon(latlonStep0)

      (height0,lat0,lon0)
    }

    val timeV=VariableOperationCW.creatVar(times,timeName)

    var emptyAttribute: mutable.HashMap[String,String]= new mutable.HashMap[String,String]
    emptyAttribute=AttributesOperationCW.updateTimeStep(emptyAttribute,timeStep)
    emptyAttribute=AttributesOperationCW.updatefcdate(emptyAttribute,fcdate)
    emptyAttribute=AttributesOperationCW.updateFcType(emptyAttribute,fcType)

    emptyAttribute=AttributesOperationCW.addAppIDTrace(emptyAttribute,appId)
    emptyAttribute=AttributesOperationCW.addIDName(emptyAttribute,timeidName)

    val dataNames:Array[String]=PropertiesUtil.getwElement(fcType,timeStep)

    val jsonHead=jsonArr.head
    val idNameHead=AccordSciDatas_JsonObjectCW.analysisJsonIdName(jsonHead)


    val heightV=VariableOperationCW.creatVar(height,heightName)
    val latV=VariableOperationCW.creatVar(lat,latName)
    val lonV=VariableOperationCW.creatVar(lon,lonName)

    //    var scidata0:SciDataset=SciDatasetOperation.empty4SciDataset(timeV,heightV,latV,lonV,dataNames,datasetName,"double",emptyAttribute)
    var scidata0:SciDatasetCW=null
    var scidata1:SciDatasetCW=null
    var i:Int=0
    jsonArr.foreach(f=>{
      val idName=AccordSciDatas_JsonObjectCW.analysisJsonIdName(f)
      println(new Date+";准备数据:idName="+idName+";timeStep="+timeStep+";i="+i)
      SystemGC.gc()
      //      scidata1=DataInterception.datacutByTime(ReadFile.ReadFcScidata(idName,timeStep,fcType,fcdate),dataNames,times)
      scidata1=ReadSciCW.ReadFcScidata(idName,timeStep,fcType,fcdate)
      println(new Date+";scidata1完毕:idName="+idName+";timeStep="+timeStep+";i="+i)
      SystemGC.gc()

      scidata0={
        if(i==0){
          val dataName_dataTypes:Array[(String,String)]=dataNames.map(dataName=>{
//            val dataType:String=scidata1.variables.get(dataName).get.dataType

            (dataName,dataType)
          })
          SciDatasetOperationCW.empty4SciDataset_DataType(timeV,heightV,latV,lonV,dataName_dataTypes,datasetName,emptyAttribute)
        }else{
          scidata0
        }
      }
      println(new Date+";scidata1准备开始:idName="+idName+";timeStep="+timeStep+";i="+i)
      SystemGC.gc()

      scidata0=DataReplaceCW.dataReplace((scidata1,scidata0),timeName,heightName,latName,lonName,dataNames)
      println(new Date+";scidata1更新完毕:idName="+idName+";timeStep="+timeStep+";i="+i)
      SystemGC.gc()
      i+=1
    })



    scidata0
  }*/

  def ReturnFlag (fcType_timesRDD: RDD[(String, Double, Array[Double])], fcdate: Date, heightStep: Double, latlonStep: Double, jsonArr: Array[JSONObject], appId: String): Boolean = {
    //    val rdd=this.reducebyLatLon2Scidata4 (fcType_timesRDD:RDD[(String,Double,Array[Double])], fcdate:Date, heightStep:Double, latlonStep:Double, jsonArr:Array[JSONObject], appId:String)

    val rdd = this.reducebyLatLon2Scidata3(fcType_timesRDD: RDD[(String, Double, Array[Double])], fcdate: Date, latlonStep: Double, jsonArr: Array[JSONObject], appId: String)

    rdd.map(scidata => {
      val timeStep = AttributesOperationCW.gettimeStep(scidata)
      val fcType = AttributesOperationCW.getFcType(scidata)
      val resultHdfsPath = PropertiesUtil.getreduceLatLonPath(timeStep, fcType, fcdate)
      WriteNcCW.WriteNcFile_addID_tmp(scidata, resultHdfsPath, appId)
    }).reduce((x, y) => x && y)
  }

  private def reducebyLatLon2Scidata3 (fcType_timesRDD: RDD[(String, Double, Array[Double])], fcdate: Date, latlonStep: Double, jsonArr: Array[JSONObject], appId: String): RDD[SciDatasetCW] = {
    fcType_timesRDD.map(fcType_times => this.reducebyLatLon2Scidata3(fcType_times, fcdate: Date, latlonStep: Double, jsonArr: Array[JSONObject], appId: String))
  }

  private def reducebyLatLon2Scidata3 (fcType_time: (String, Double, Array[Double]), fcdate: Date, latlonStep: Double, jsonArr: Array[JSONObject], appId: String): SciDatasetCW = {
    val fcType = fcType_time._1
    val timeStep: Double = fcType_time._2
    val times = fcType_time._3
    //    println(fcType+","+timeStep+","+ShowUtil.ArrayShowStr(times))
    //    val timeidName = times.min + "_(" + timeStep + ")_" + times.max

    val splitNum = jsonArr.length

    //    val datasetName=times.min+"_"+timeStep+"_"+times.max+"_"+fcType+"_"+splitNum+".nc"
    val (dataType: String, radio0: Int) = PropertiesUtil.reduceLatLon_DataTypeRadio(fcType)

    val datasetName = {
      if (fcType.equals(Constant.WINUV)) {
        this.dataNameFun(fcdate, Constant.WIND, radio0, times.min, times.max, timeStep)
      } else {
        this.dataNameFun(fcdate, fcType, radio0, times.min, times.max, timeStep)
      }
    }

    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val (lat: Array[Double], lon: Array[Double]) = {
      val Var = jsonArr.map(json => {
        val (idName, timeBegin, timeEnd, timeStep, heightBegin, heightEnd, heightStep, latBegin, latEnd, latStep, lonBegin, lonEnd, lonStep) = Analysis.analysisJsonProperties(json)
        (heightBegin, heightEnd, latBegin, latEnd, lonBegin, lonEnd)
      }).reduce((x, y) => {
        (Math.min(x._1, y._1), Math.max(x._2, y._2), Math.min(x._3, y._3), Math.max(x._4, y._4), Math.min(x._5, y._5), Math.max(x._6, y._6))
      })



      //      val height0=ArrayOperation.ArithmeticArray(Var._1,Var._2,heightStep)
      //      val lat0=ArrayOperation.ArithmeticArray(Var._3,Var._4,latlonStep)
      //      val lon0=ArrayOperation.ArithmeticArray(Var._5,Var._6,latlonStep)

      val latlonStep0: Double = 0.01d
      val (lat0, lon0) = PropertiesUtil.getLatLon(latlonStep0)

      (lat0, lon0)
    }

    val timeV = VariableOperationCW.creatVar(times, timeName)

    var emptyAttribute: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    emptyAttribute = AttributesOperationCW.updateTimeStep(emptyAttribute, timeStep)
    emptyAttribute = AttributesOperationCW.updatefcdate(emptyAttribute, fcdate)
    emptyAttribute = AttributesOperationCW.updateFcType(emptyAttribute, fcType)

    emptyAttribute = AttributesOperationCW.addAppIDTrace(emptyAttribute, appId)
    //    emptyAttribute = AttributesOperationCW.addIDName(emptyAttribute, timeidName)

    val dataNames: Array[String] = PropertiesUtil.getwElement(fcType, timeStep)

    //    val jsonHead = jsonArr.head
    //    val idNameHead = AccordSciDatas_JsonObjectCW.analysisJsonIdName(jsonHead)

    //lat,lon修改数据类型
    val latDataType: String = PropertiesUtil.reduceLatLon_DataTypeRadio(latName)._1
    val lonDataType: String = PropertiesUtil.reduceLatLon_DataTypeRadio(lonName)._1

    val latV = VariableOperationCW.creatVar(lat, latName, latDataType)
    val lonV = VariableOperationCW.creatVar(lon, lonName, lonDataType)

    //    var scidata0:SciDataset=SciDatasetOperation.empty4SciDataset(timeV,heightV,latV,lonV,dataNames,datasetName,"double",emptyAttribute)
    var scidata0: SciDatasetCW = null
    var scidata1: SciDatasetCW = null
    var i: Int = 0
    jsonArr.foreach(f => {
      val idName = Analysis.analysisJsonIdName(f)
      println(new Date + ";准备数据:idName=" + idName + "fcType=" + fcType + ";timeStep=" + timeStep + ";i=" + i)
      //      scidata1=DataInterception.datacutByTime(ReadFile.ReadFcScidata(idName,timeStep,fcType,fcdate),dataNames,times)
      scidata1 = {
        val orginalSci: SciDatasetCW = {
          ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdate)
        }
        val radio = math.pow(10, radio0)
        DataOffsetCW.dataoffset(orginalSci, dataNames, radio, 0)
        //        orginalSci
      }
      println(new Date + ";scidata1完毕:idName=" + idName + ";timeStep=" + timeStep + ";i=" + i)

      scidata0 = {
        if (i == 0) {
          val dataName_dataTypes: Array[(String, String)] = dataNames.map(dataName => {
            //            val dataType:String=scidata1.variables.get(dataName).get.dataType

            (dataName, dataType)
          })
          SciDatasetOperationCW.empty3SciDataset_DataType(timeV, latV, lonV, dataName_dataTypes, datasetName, emptyAttribute)
        } else {
          scidata0
        }
      }
      println(new Date + ";scidata1准备开始:idName=" + idName + ";timeStep=" + timeStep + ";i=" + i)

      scidata0 = DataReplaceCW.dataReplace1((scidata1, scidata0), timeName, latName, lonName, dataNames)
      println(new Date + ";scidata1更新完毕:idName=" + idName + ";timeStep=" + timeStep + ";i=" + i)
      i += 1
    })

    //    val dataNames=PropertiesUtil.getwElement(FcType,timeStep)
    //改名
    dataNames.foreach(dataName => {
      val dataV: VariableCW = scidata0.variables.find(p => p._1.equals(dataName)).get._2
      val newName = PropertiesUtil.getNMCdataName(dataName, timeStep)
      dataV.setName(newName)
    })

    AttributesOperationCW.setReftimeOpen(scidata0, true)

    scidata0
  }

  private def dataNameFun (fcdate: Date, fcType: String, scale_factor: Int, t0: Double, t1: Double, timeStep: Double): String = {
    val sourceType = "nmc"
    val h0 = 0f
    val h1 = 0f
    val LatLon = "0f,60f,70f,140f"
    val heightStep = 0f
    val LatLonStep = 0.01f
    //    val scale_factor :Int= {
    //      if(fcType.equals(ConstantUtil.TEM) || fcType.equals(ConstantUtil.PRE)){
    //        1
    //      }else{
    //        0
    //      }
    //    }
    this.GetNcFileName(sourceType, fcType, t0.toFloat, t1.toFloat, h0, h1, LatLon, timeStep.toFloat, heightStep, LatLonStep, scale_factor, fcdate)
  }

  private def GetNcFileName (sourceType: String, FcType: String, t0: Float, t1: Float, h0: Float, h1: Float, LatLon: String, timeStep: Float, heighStep: Float, LatLonStep: Float, scale_factor: Int, date: Date): String = { //"nmc_"+(int)timeStep+"h_CLOUD_"+String.valueOf((int)times2[i][0])+"_0_0_70_1_0_0.01_0.01_"+(String.valueOf((int)times2[i][times2[0].length-1]))+"_0_60_140_0_201707130800"
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
