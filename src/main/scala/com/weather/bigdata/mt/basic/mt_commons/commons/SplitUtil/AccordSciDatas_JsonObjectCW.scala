package com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesConstant, AttributesOperationCW}
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{IndexOperation, VariableOperationCW, ucarma2ArrayOperationCW}
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.reNameUtil.reName_AfterSplitCW
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AccordSciDatas_JsonObjectCW {


  def AccSciRdd_reName (JSONObjectRDD: RDD[JSONObject], unSplitesciData: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, FcType: String, fcdate: Date): RDD[SciDatasetCW] = {
    val rdd = this.AccSciRdd(JSONObjectRDD: RDD[JSONObject], unSplitesciData: SciDatasetCW, (timeName, heightName, latName, lonName), FcType)
    reName_AfterSplitCW.reName_Variable(rdd, timeName, heightName, latName, lonName, FcType, fcdate)
  }

  def AccSciRdd (JSONObjectRDD: RDD[JSONObject], unSplitesciData: SciDatasetCW, NewVarsName: (String, String, String, String), FcType: String): RDD[SciDatasetCW] = {
    val sc = ContextUtil.getSparkContext( )
    val unSplitesciDataValue: Broadcast[SciDatasetCW] = sc.broadcast(unSplitesciData)
    //    val unSplitesciDataValuevalue=unSplitesciDataValue.value

    val rdd = JSONObjectRDD.map(j => {
      val start = System.currentTimeMillis( )

      val dataNames = PropertiesUtil.getrElement(FcType)

      val idName: String = Analysis.analysisJsonIdName(j)
      val (timeName, heightName, latName, lonName) = NewVarsName
      val SplitVars = this.AccSciGetVars(j, unSplitesciDataValue.value)
      val mainV: ArrayBuffer[VariableCW] = this.AccSciGetMainV(SplitVars, unSplitesciDataValue.value.variables.values, timeName, heightName, latName, lonName, dataNames)
      val sciDatasetName_old = unSplitesciDataValue.value.datasetName
      val sciDatasetName_new = SplitMatchInfo.oldName2newName(sciDatasetName_old, idName)
      //      val varHash:ArrayBuffer[(String,VariableCW)]=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,mainV,(timeName,heightName,latName,lonName))

      var att: mutable.HashMap[String, String] = unSplitesciDataValue.value.attributes
      //      var att :mutable.HashMap[String,String] = new mutable.HashMap[String,String]()
      att = AttributesOperationCW.addTrace(att, "Accord[rddID(" + idName + ")]")
      att = AttributesOperationCW.addIDName(att, idName)
      att = AttributesOperationCW.coverInfo(att, AttributesConstant.fcTypeKey, FcType)

      val scidatanew = new SciDatasetCW(mainV, att, sciDatasetName_new)

      val end = System.currentTimeMillis( )
      WeatherShowtime.showDateStrOut1("AccSciRdd", start, end, sciDatasetName_old + "=>" + sciDatasetName_new)
      scidatanew
    })
    unSplitesciDataValue.unpersist( )
    rdd
  }

  private def AccSciGetMainV (SpliteVals: ArrayBuffer[VariableCW], unSpliteVals: Iterable[VariableCW], unSplitetimeName: String, unSpliteheightName: String, unSplitelatName: String, unSplitelonName: String, unSplitdataNames: Array[String]): ArrayBuffer[VariableCW] = {

    val fileVals = new ArrayBuffer[VariableCW]( )

    val SplittimeV: VariableCW = VariableOperationCW.getVar(SpliteVals, unSplitetimeName)
    var SplitheightV: VariableCW = VariableOperationCW.getVar(SpliteVals, unSpliteheightName)
    val SplitlatV: VariableCW = VariableOperationCW.getVar(SpliteVals, unSplitelatName)
    val SplitlonV: VariableCW = VariableOperationCW.getVar(SpliteVals, unSplitelonName)

    val unSplittimeVList = unSpliteVals.filter(_.name == unSplitetimeName)
    val unSplitheightVList = unSpliteVals.filter(_.name == unSpliteheightName)
    val unSplittimeV: VariableCW = {
      if (!unSplittimeVList.isEmpty) {
        unSplittimeVList.toList(0)
      } else {
        SplittimeV
      }
    }

    val unSplitheightV: VariableCW = {
      if (!unSplitheightVList.isEmpty) {
        val unSplitheightV0 = unSplitheightVList.toList.last
        if (unSplitheightV0.dataDouble( ).length == 1) {
          SplitheightV = unSplitheightV0
        }
        unSplitheightV0
      } else {
        SplitheightV
      }
    }


    val unSplitlatV: VariableCW = unSpliteVals.find(p => p.name.equals(unSplitelatName)).get
    val unSplitlonV: VariableCW = unSpliteVals.find(p => p.name.equals(unSplitelonName)).get

    val timeV: VariableCW = this.decideVar(SplittimeV, unSplittimeV)
    val heighV: VariableCW = this.decideVar(SplitheightV, unSplitheightV)
    val latV: VariableCW = this.decideVar(SplitlatV, unSplitlatV)
    val lonV: VariableCW = this.decideVar(SplitlonV, unSplitlonV)

    fileVals += timeV += latV += lonV += heighV

    val unSplitdata: Array[VariableCW] = unSplitdataNames.map(unSplitdataName => {
      val unSplitdataV: VariableCW = unSpliteVals.filter(_.name == unSplitdataName).toList(0)
      this.getdataV(SplittimeV, SplitheightV, SplitlatV, SplitlonV, unSplittimeV, unSplitheightV, unSplitlatV, unSplitlonV, unSplitdataV)
    })
    fileVals ++= unSplitdata
    fileVals
  }

  private def decideVar (SplitV: VariableCW, unSplitV: VariableCW): VariableCW = SplitV

  private def AccSciGetVars(JSONPro: JSONObject, unSplitscidata: SciDatasetCW): ArrayBuffer[VariableCW] = {
    val (idName: String, timeBegin: Double, timeEnd: Double, timeStep: Double, heightBegin: Double, heightEnd: Double, heightStep: Double, latBegin: Double, latEnd: Double, latStep: Double, lonBegin: Double, lonEnd: Double, lonStep: Double) = Analysis.analysisJsonProperties(JSONPro)

    //    val (timeName,heightName,latName,lonName)=AttributesOperation.getVarName(unSplitscidata)
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()

    val timeV = {
      if (unSplitscidata.variables.contains(timeName)) {
        val untimeV = unSplitscidata.apply(timeName)
        VariableOperationCW.creatVar(timeBegin, timeEnd, timeStep, untimeV)
      } else {
        VariableOperationCW.creatVar(timeBegin, timeEnd, timeStep, timeName)
      }
    }
    val heightV = {
      if (unSplitscidata.variables.contains(heightName)) {
        val unheightV = unSplitscidata.apply(heightName)
        VariableOperationCW.creatVar(heightBegin, heightEnd, heightStep, unheightV)
      } else {
        VariableOperationCW.creatVar(heightBegin, heightEnd, heightStep, heightName)
      }
    }
    val latV = {
      if (unSplitscidata.variables.contains(latName)) {
        val unlatV = unSplitscidata.apply(latName)
        VariableOperationCW.creatVar(latBegin, latEnd, latStep, unlatV)
      } else {
        VariableOperationCW.creatVar(latBegin, latEnd, latStep, latName)
      }

    }
    val lonV = {
      if (unSplitscidata.variables.contains(lonName)) {
        val unlonV = unSplitscidata.apply(lonName)
        VariableOperationCW.creatVar(lonBegin, lonEnd, lonStep, unlonV)
      } else {
        VariableOperationCW.creatVar(lonBegin, lonEnd, lonStep, lonName)
      }
    }

    val fileVals = new ArrayBuffer[VariableCW]()
    fileVals += timeV += heightV += latV += lonV
    fileVals
  }

  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )
    val jsonFile = PropertiesUtil.getjsonFile( )
    val jsonRdd = ShareData.jsonRdd_ExistOrCreat(jsonFile)
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val NewVarsName = PropertiesUtil.getVarName( )


    var unSplitesciData = ReadNcCW.ReadNcScidata("/D:/Data/forecast/Input/Real/20180308/08/TMP/2018030808_TMP.nc")
    unSplitesciData = AttributesOperationCW.addVarName(unSplitesciData, (timeName, heightName, latName, lonName))
    unSplitesciData = AttributesOperationCW.setDataNames(unSplitesciData, Array("Temperature_height_above_ground"))
    val rdd = this.AccSciRdd(jsonRdd, unSplitesciData, NewVarsName, "TEM")
    WriteNcCW.WriteNcFile_test(rdd, "/D:/tmp/aftersplit/")
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("AccordSciDatas_JsonObjectCW", start, end)
  }

  private def getdataV (SplittimeV: VariableCW, SplitheightV: VariableCW, SplitlatV: VariableCW, SplitlonV: VariableCW, unSplittimeV: VariableCW, unSplitheightV: VariableCW, unSplitlatV: VariableCW, unSplitlonV: VariableCW, unSplitdataV: VariableCW): VariableCW = {

    //    val array=unSplitdataV.apply()


    val unSplittime = unSplittimeV.dataFloat
    val unSplitheight = unSplitheightV.dataFloat
    val unSplitlat = NumOperation.Kdec(unSplitlatV.dataFloat)
    val unSplitlon = NumOperation.Kdec(unSplitlonV.dataFloat)

    val Splittime = SplittimeV.dataFloat
    val Splitheight = SplitheightV.dataFloat
    val Splitlat = NumOperation.Kdec(SplitlatV.dataFloat)
    val Splitlon = NumOperation.Kdec(SplitlonV.dataFloat)

    val SplittimeLen = SplittimeV.getLen( )
    val SplitheightLen = SplitheightV.getLen( )
    val SplitlatLen = SplitlatV.getLen( )
    val SplitlonLen = SplitlonV.getLen( )

    val unSplittimeLen = unSplittimeV.getLen( )
    val unSplitheightLen = unSplitheightV.getLen( )
    val unSplitlatLen = unSplitlatV.getLen( )
    val unSplitlonLen = unSplitlonV.getLen( )


    var ei, ej, ek, el: Exception = null
    val IArray: Array[Int] = {
      try {
        ArrayOperation.HashMapSearch(unSplittime, Splittime)
      } catch {
        case e: Exception => {
          ei = e
          null
        }
      }
    }
    val JArray: Array[Int] = {
      try {
        ArrayOperation.HashMapSearch(unSplitheight, Splitheight)
      } catch {
        case e: Exception => {
          ej = e
          null
        }
      }
    }
    val KArray: Array[Int] = {
      try {
        ArrayOperation.HashMapSearch(unSplitlat, Splitlat)
      } catch {
        case e: Exception => {
          ek = e
          null
        }
      }
    }
    val LArray: Array[Int] = {
      try {
        ArrayOperation.HashMapSearch(unSplitlon, Splitlon)
      } catch {
        case e: Exception => {
          el = e
          null
        }
      }
    }

    val dataTypeStr = unSplitdataV.dataType

    val shape = Array(SplittimeLen, SplitheightLen, SplitlatLen, SplitlonLen)
    val data: ucar.ma2.Array = ucar.ma2.Array.factory(DataType.getType(dataTypeStr), shape)

    var index0, index: Int = 0
    if (ei == null && ej == null && ek == null && el == null) {
      for (i <- 0 to SplittimeLen - 1) {
        for (j <- 0 to SplitheightLen - 1) {
          for (k <- 0 to SplitlatLen - 1) {
            for (l <- 0 to SplitlonLen - 1) {
              index0 = IndexOperation.getIndex4_thlatlon(IArray(i), JArray(j), KArray(k), LArray(l), unSplitheightLen, unSplitlatLen, unSplitlonLen)
              index = IndexOperation.getIndex4_thlatlon(i, j, k, l, SplitheightLen, SplitlatLen, SplitlonLen)
              ucarma2ArrayOperationCW.setObject(data, index, unSplitdataV.dataObject(index0))
            }
          }
        }
      }
      val endTime = System.currentTimeMillis( )
      println("AccordSciDatas_JsonObjectCW未出现匹配异常")

    } else {
      val e: Exception = new Exception("====分割未成功,匹配异常,数据为空--->")
      e.printStackTrace( )
      if (ei != null) {
        val msg = this.getClass.getName + ":time match error,"
        val e0: Exception = new Exception(msg + ei)
        e0.printStackTrace( )
      }
      if (ej != null) {
        val msg = this.getClass.getName + ":height match error,"
        val e0: Exception = new Exception(msg + ej)
        e0.printStackTrace( )
      }
      if (ek != null) {
        val msg = this.getClass.getName + ":lat match error,"
        val e0: Exception = new Exception(msg + ek)
        e0.printStackTrace( )
      }
      if (el != null) {
        val msg = this.getClass.getName + ":lon match error,"
        val e0: Exception = new Exception(msg + el)
        e0.printStackTrace( )
      }
    }

    //    val shape = Array(SplittimeLen,SplitheightLen, SplitlatLen, SplitlonLen)
    //    val dims = List((SplittimeV.name, SplittimeLen),(SplitheightV.name, SplitheightLen) ,(SplitlatV.name, SplitlatLen), (SplitlonV.name, SplitlonLen))

    val VarName = unSplitdataV.name

    val att = unSplitdataV.attributes
    //    new VariableCW(VarName, dataTypeStr , new Nd4jTensor(data, shape),att , dims)
    VariableOperationCW.creatVars4(data, VarName, SplittimeV.name, SplitheightV.name, SplitlatV.name, SplitlonV.name)
  }
}
