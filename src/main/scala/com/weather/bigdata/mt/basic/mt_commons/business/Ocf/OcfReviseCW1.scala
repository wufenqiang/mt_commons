package com.weather.bigdata.mt.basic.mt_commons.business.Ocf

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil.NcRevisedByOcfCW
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object OcfReviseCW1{
  private val checkStationOpen: Boolean = PropertiesUtil.checkStationOpen
  /*private def ocfrevise(jsonRdd:RDD[JSONObject],ocfdate:Date,timeStep:Double,sciGeo:SciDataset,ocffile:String,FcType:String): Boolean = {

    val appID=ContextUtil.getApplicationID
    val ocfMap:Map[String, Map[String,Map[String,Double]]] = ReadFile.ReadOcfFile(ocffile,timeStep,ocfdate,appID)

    val ocfdateStr=DataFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)
    val fcdate=WeatherDate.OcfGetFc(ocfdate)

    val para="FcType="+FcType+";ocfdateStr="+ocfdateStr+";timeStep="+timeStep.toInt
//    val jsonRdd=jsonRdds._1

    val Start=System.currentTimeMillis()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>appID="+appID+";OcfRevise.ocfrevise.Start:("+para+")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    val rdd=ReadFile.ReadFcRdd(jsonRdd,timeStep,FcType,fcdate)

    val afterOcf=NcRevisedByOcf.RevisedByCity(rdd,FcType,ocfdate,timeStep,sciGeo,ocfMap)

//    val fcpath=PropertiesUtil.getfcPath(timeStep,FcType,fcdate)
    val flag=WriteFile.WriteFcFile_addId_tmpReturn(afterOcf,appID)

    val End=System.currentTimeMillis()
    ws.showDateStrOut1("ocfrevise:",Start,End)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>appID="+appID+";OcfRevise.ocfrevise.End:("+para+")<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    flag

  }*/

  def ReturnData (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)]): RDD[SciDatasetCW] = {
    val sc = ContextUtil.getSparkContext( )
    val sciGeoBro: Broadcast[SciDatasetCW] = sc.broadcast(sciGeo)
    val appID = ContextUtil.getApplicationID

    rdds.map(scidata => this.ReturnData(scidata, sciGeoBro.value, ocffileArr, appID))
  }

  def ReturnFlag (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val appID = ContextUtil.getApplicationID
    //    val sc = ContextUtil.getSparkContext( )
    //    val sciGeoBro: Broadcast[SciDatasetCW] = sc.broadcast(sciGeo)

    val afterOcfRdd = this.ReturnData_plusStation(rdds,sciGeo, ocffileArr, stationsInfos)
    val flag: Boolean = afterOcfRdd.map(scidata => WriteNcCW.WriteFcFile_addId_tmp(scidata, appID)).reduce((x, y) => (x & y))

    flag
  }

  def ReturnData_plusStation (rdds: RDD[SciDatasetCW], sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] = {
    val sc = ContextUtil.getSparkContext( )
    val sciGeoBro: Broadcast[SciDatasetCW] = sc.broadcast(sciGeo)

    val appID = ContextUtil.getApplicationID
    rdds.map(scidata => this.ReturnData_plusStation(scidata, sciGeoBro.value, ocffileArr, stationsInfos, appID))
  }

  def ReturnData_plusStation (scidata: SciDatasetCW, sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], stationsInfos: (mutable.HashMap[String, String], String, Date), appID: String): SciDatasetCW = {
    val scidata0:SciDatasetCW = this.ReturnData(scidata: SciDatasetCW, sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], appID: String)
    if (checkStationOpen) {
      val fcdate = AttributesOperationCW.getfcdate(scidata)
      val stationsInfo: mutable.HashMap[String, String] = stationsInfos._1
      val stationocf: String = stationsInfos._2
      val ocfdate = stationsInfos._3
      CheckStationCW.checkStation(appID, stationsInfo, stationocf, scidata, ocfdate, fcdate)
    } else {
      println("CheckStation开关关闭:" + checkStationOpen)
    }
    scidata0
  }

  private def ReturnData (scidata: SciDatasetCW, sciGeo: SciDatasetCW, ocffileArr: Array[(Double, Date, String)], appID: String): SciDatasetCW = {
    val timeStep = AttributesOperationCW.gettimeStep(scidata)
    val fcType = AttributesOperationCW.getFcType(scidata)
    val ocffiles: (Double, Date, String) = {
      try {
        println("ocffileArr get timeStep=" + timeStep)
        ocffileArr.find(p => p._1 == timeStep).get
      } catch {
        case e: Exception => {
          e.printStackTrace( )
          null
        }
      }
    }
    val ocfdate = ocffiles._2
    val ocffile = ocffiles._3
    val ocfMap : Map[String, Map[String, Map[String, Float]]]= ReadFile.ReadOcfFile0(ocffile, timeStep, ocfdate, appID)

    /*val ocfMaps:(Double,Date,Map[String, Map[String,Map[String,Double]]])=ocfMapValue.value.find(p=>p._1==timeStep).get
    val ocfMap=ocfMaps._3*/


    val afterOcf: SciDatasetCW = NcRevisedByOcfCW.RevisedByCity(scidata, fcType, ocfdate, timeStep, sciGeo, ocfMap)

    afterOcf
  }

  /*def ReturnRdd (rdds:RDD[SciDataset], sciGeo:SciDataset, ocffileArr:Array[(Double,Date,String)], stationsInfos:(mutable.HashMap[String,String],String,Date)): RDD[SciDataset] ={
    this.ReturnData_plusStation(rdds, sciGeo, ocffileArr, stationsInfos)
  }*/

  //  private def mainocf(jsonFile:String, timeStamp:Date, timeSteps:Array[Double], sciGeo:SciDataset, ocffileArr:Array[(Double,Date,String)],stationsInfos:(mutable.HashMap[String,String],String,Date),fcTypes:Array[String]): Boolean ={
  //    /*//测试ocfdate的统一性
  //    {
  //      val ocfdate0=ocfdate
  //      val ocfdate1s:Array[Date]=ocffileArr.map(s=>s._2)
  //      ocfdate1s.foreach(ocfdate1=>{
  //        if(!DateOperation.equal(ocfdate0,ocfdate1)){
  //          val e:Exception=new Exception("ocfdate的统一性有误;ocfdate0="+DataFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate0)+"ocfdate1="+DataFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate1))
  //          e.printStackTrace()
  //          throw e
  //        }
  //      })
  //    }*/
  //
  //    //使用信号时间戳计算入库对应的时间
  //    val fcdate=WeatherDate.ObsGetFc(timeStamp)
  //    //    val fcdate=WeatherDate.OcfGetFc(ocfdate)
  //
  //    //    val fcTypes_excWEATHER=fcTypes.filter(p=>(!p.equals(ConstantUtil.WEATHER)))
  //
  //    val rdds:RDD[SciDataset]=ReadFile.ReadFcRdd(jsonFile,timeSteps,fcTypes,fcdate)
  //
  //    val flag:Boolean=this.ReturnFlag(rdds,sciGeo,ocffileArr,stationsInfos)
  //
  //    flag
  //  }

  def main (args: Array[String]): Unit = {

  }
}
