package com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.ReadWrite
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW}
import org.apache.spark.rdd.RDD

object ShareData {
  private var sciGeoCW: SciDatasetCW = null
  private var jsonRdd: RDD[JSONObject] = null
  private var jsonArr: Array[JSONObject] = null
  private var splitNum: Int = -1
  private var splitNum_local: Int = -1

  def sciGeoCW_ExistOrCreat: SciDatasetCW = {
    if (this.getShareOpen) {
      if (sciGeoCW == null) {
        sciGeoCW = ReadNcCW.ReadGeoFile()
      }
      sciGeoCW
    } else {
      ReadNcCW.ReadGeoFile()
    }
  }

  def jsonRdd_ExistOrCreat(splitFile: String): RDD[JSONObject] = {
    if (this.getShareOpen) {
      if (jsonRdd == null) {
        jsonRdd = ReadFile.ReadJsonRdd(splitFile)
      }
      jsonRdd
    } else {
      ReadFile.ReadJsonRdd(splitFile)
    }
  }

  private def getShareOpen: Boolean = {
    val shareOpen: Boolean = false
    println("shareOpen=" + shareOpen)
    shareOpen
  }


  def splitNum_ExistOrCreat(splitFile: String): Int = {
    if (this.getShareOpen) {
      if (splitNum == -1) {
        splitNum = jsonArr_ExistOrCreat(splitFile).length
      }
      splitNum
    } else {
      jsonArr_ExistOrCreat(splitFile).length
    }
  }

  def jsonArr_ExistOrCreat (jsonFile: String): Array[JSONObject] = {
    if (this.getShareOpen) {
      if (jsonArr == null) {
        jsonArr = ReadFile.ReadJsonArr(jsonFile)
      }
      jsonArr
    } else {
      ReadFile.ReadJsonArr(jsonFile)
    }
  }

  def splitNumlocal_ExistOrCreat (jsonFile_local: String): Int = {
    if (this.getShareOpen) {
      if (splitNum_local == -1) {
        splitNum_local = ReadWrite.ReadTXT2ArrString(jsonFile_local).length
      }
      splitNum_local
    } else {
      ReadWrite.ReadTXT2ArrString(jsonFile_local).length
    }
  }

}

/*object ShareData{
  private val sd=new ShareData
  def sciGeo_ExistOrCreat:SciDataset=this.sd.sciGeo_ExistOrCreat
  def jsonRdd_ExistOrCreat(jsonFile:String):RDD[JSONObject]=this.sd.jsonRdd_ExistOrCreat(jsonFile)
  def jsonArr_ExistOrCreat(jsonFile:String): Array[JSONObject] =this.sd.jsonArr_ExistOrCreat(jsonFile)
  def splitNum_ExistOrCreat(jsonFile:String):Int=this.sd.splitNum_ExistOrCreat(jsonFile)
  def splitNumlocal_ExistOrCreat(jsonFile:String):Int=this.sd.splitNumlocal_ExistOrCreat(jsonFile)
}*/
