package com.weather.bigdata.mt.basic.mt_commons.business.Obs

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.mt.basic.mt_commons.business.Fc.FcMainCW
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

private object Input1hRddCW {
  def input1hRdd(splitFile: String, fcTypes: Array[String], obsdate: Date): RDD[SciDatasetCW] = this.input1hRdd0(splitFile, fcTypes, obsdate)

  //磁盘文件为输入源
  private def input1hRdd0(splitFile: String, fcTypes: Array[String], obsdate: Date): RDD[SciDatasetCW] = {
    val timeStep: Double = 1.0d
    val fcdate = WeatherDate.ObsGetFc(obsdate)
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(splitFile)
    fcTypes.map(
      fcType => {
        jsonRdd.map(
          jpo => {
            val idName: String = Analysis.analysisJsonIdName(jpo)
            val scidata = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdate)
            AttributesOperationCW.updateFcType(scidata, fcType)
          }
        )
      }
    ).reduce((x, y) => (x.union(y)))
  }

  //从原始数据3h作为输入源
  private def input1hRdd1(jsonRdd: RDD[JSONObject], fcTypes: Array[String], obsdate: Date): RDD[SciDatasetCW] = {
    val appID: String = ContextUtil.getApplicationID

    val rdd = {
      val timeStep: Double = 1.0d
      val fcdate = WeatherDate.ObsGetFc(obsdate)

      val gr2bFrontOpen = PropertiesUtil.gr2bFrontOpen

      fcTypes.map(fcType => {

        val ExtremMap = {
          if (fcType.equals(Constant.TEM)) {
            ReadFile.ReadTemExFile(PropertiesUtil.getTemExtremPath())
          } else {
            Map(): Map[String, Map[String, Array[Float]]]
          }
        }

        val gr2bfileName: String = PropertiesUtil.getgr2bFile(fcType, fcdate)

        FcMainCW.nmc2fcRdd(jsonRdd, fcType, fcdate, gr2bfileName, ExtremMap, gr2bFrontOpen, timeStep, appID)
      }).reduce((x, y) => x.union(y))
    }

    rdd
  }

  //从原始数据3h作为输入源后再进行ocf订正
  //  private def input1hRdd2(jsonRdd:RDD[JSONObject],fcTypes:Array[String],obsdate:Date): RDD[SciDataset] ={
  //    val timeStep:Double=1.0d
  //    val fcdate=WeatherDate.ObsGetFc(obsdate)
  //
  //    val gr2bFrontOpen=PropertiesUtil.gr2bFrontOpen
  //
  //    var rdd0=fcTypes.map(fcType=>{
  //
  //      val ExtremMap={
  //        if(fcType.equals(ConstantUtil.TEM)){
  //          ReadFile.ReadTemExFile(PropertiesUtil.getTemExtremPath())
  //        }else{
  //          Map():Map[String,Map[String,Array[Double]]]
  //        }
  //      }
  //
  //      val gr2bfileName:String=PropertiesUtil.getgr2bFile(fcType,fcdate)
  //
  //      FcMain.nmc2fcRdd(jsonRdd, fcType, fcdate, gr2bfileName, ExtremMap,gr2bFrontOpen, timeStep)
  //    }).reduce((x,y)=>x.union(y))
  //
  //
  //    rdd0={
  //      val ocfdate=WeatherDate.ObsGetOcf(obsdate)
  //      val ocffile= PropertiesUtil.getocffile(ocfdate,timeStep)
  //      val ocffileArr=Array((timeStep,ocfdate,ocffile))
  //      val sciGeo = ReadFile.ReadGeoFile()
  //      OcfRevise.ReturnData(rdd0,sciGeo,ocffileArr)
  //    }
  //
  //    rdd0
  //  }
}