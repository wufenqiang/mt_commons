package com.weather.bigdata.mt.basic.mt_commons.commons.reNameUtil

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesConstant, AttributesOperationCW}
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.VariableOperationCW
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.{ShowUtil, WeatherShowtime}
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.SplitMatchInfo
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

object reName_AfterSplitCW {
  private val VO = VariableOperationCW

  def reName_Variable (rdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, FcType: String, fcdate: Date): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.reName_Variable(scidata, timeName, heightName, latName, lonName, FcType, fcdate))
  }

  def reName_Variable (scidata: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, fcType: String, fcdate: Date): SciDatasetCW = {
    val wdataNames: Array[String] = PropertiesUtil.getrElement_rename(fcType)
    val rdataNames: Array[String] = PropertiesUtil.getrElement(fcType)
    if (wdataNames.length != rdataNames.length) {
      val e: Exception = new Exception("reName,rdataNames与wdataNames不匹配")
      e.printStackTrace( )
      ShowUtil.ArrayShowError(wdataNames)
      ShowUtil.ArrayShowError(rdataNames)
      scidata
    } else {
      //        val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
      //      var scidata0 = scidata
      val timeV = scidata.apply(timeName)
      val heightV = scidata.apply(heightName)
      val latV = scidata.apply(latName)
      val lonV = scidata.apply(lonName)
      //      val datasV: Array[VariableCW] = new Array[VariableCW](rdataNames.length)
      //      val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]
      //      vals += timeV += heightV += latV += lonV
      for (i <- 0 to rdataNames.length - 1) {
        val v: VariableCW = scidata.apply(rdataNames(i))
        v.setName(wdataNames(i))
        AttributesOperationCW.addbeforeName(v, rdataNames(i))

        scidata.variables.remove(rdataNames(i))
        scidata.update(wdataNames(i), v)
      }
      val idName = AttributesOperationCW.getIDName(scidata)
      val oldName = scidata.datasetName
      val newName = SplitMatchInfo.oldName2newName(fcType, idName)
      //      val varHash=VO.allVars2NameVar(this.getClass.toString,idName,vals,timeName,heightName,latName,lonName)
      scidata.setName(newName)

      //      val dateStr=DataFormatUtil.YYYYMMDDHHMMSSStr0(date)
      AttributesOperationCW.updatefcdate(scidata, fcdate)
      AttributesOperationCW.updateFcType(scidata, fcType)
      AttributesOperationCW.addbeforeName(scidata, oldName)
      val reNameStr = "reName[" + oldName + "->" + newName + "]"
      println(reNameStr)
      AttributesOperationCW.addTrace(scidata, reNameStr)
      AttributesOperationCW.addVarName(scidata, (timeName, heightName, latName, lonName))
      AttributesOperationCW.setDataNames(scidata, wdataNames)
      //      var attrnew0=AttributesOperation.addfcdate(scidata.attributes,date)
      /*      attrnew0=AttributesOperation.updateFcType(attrnew0,fcType)
            attrnew0=AttributesOperation.addbeforeName(attrnew0,scidata.datasetName)
            attrnew0=AttributesOperation.addTrace(attrnew0,"reName["+oldName+"->"+newName+"]")
            attrnew0=AttributesOperation.addVarName(attrnew0,timeName,heightName,latName,lonName)
            new SciDataset(varHash,attrnew0,newName)*/
      scidata
    }
  }

  def OnlyreName_AfterObs (rdd: RDD[SciDatasetCW], FcType: String, obsdate: Date, timeStep: Double): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.OnlyreName_AfterObs(scidata, FcType, obsdate, timeStep))
  }

  def OnlyreName_AfterObs (scidata: SciDatasetCW, FcType: String, obsdate: Date, timeStep: Double): SciDatasetCW = {
    val idName = AttributesOperationCW.getIDName(scidata)
    val newName = SplitMatchInfo.oldName2newName(FcType, idName, timeStep)
    val attnew = AttributesOperationCW.addInfoStr(scidata.attributes, AttributesConstant.obsdateKey, DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate))
    scidata.update(AttributesConstant.obsdateKey, attnew)
    scidata.setName(newName)
  }

  def OnlyreName (rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.OnlyreName(scidata))
  }

  def OnlyreName (scidata: SciDatasetCW): SciDatasetCW = {
    val idName = AttributesOperationCW.getIDName(scidata)
    val vash = scidata.variables
    val fcType = AttributesOperationCW.getFcType(scidata)
    val timeStep = AttributesOperationCW.gettimeStep(scidata)
    val oldName = scidata.datasetName
    val newName = SplitMatchInfo.oldName2newName(fcType, idName, timeStep)
    //    val att=scidata.attributes
    val newStr0: String = AttributesOperationCW.addTraceStr(scidata.attributes, "OnlyreName[" + oldName + "=>" + newName + "]")
    val newStr1: String = AttributesOperationCW.addInfoStr(scidata.attributes, AttributesConstant.beforeNameKey, oldName)
    scidata.update(AttributesConstant.traceKey, newStr0)
    scidata.update(AttributesConstant.beforeNameKey, newStr1)
    scidata.setName(newName)
    scidata
  }

  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName( )
    val fcType = "TEM"
    val fcdate = DateFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
    val ssc = ContextUtil.getSciSparkContextCW( )
    //    val ssc=ContextUtil.getSciSparkContext()
    val fileName = "/D:/tmp/aftersplit/"
    val rdd0: RDD[SciDatasetCW] = ssc.sciDatasets(fileName)
    val rdd1 = this.reName_Variable(rdd0, timeName, heightName, latName, lonName, fcType, fcdate)
    val flag = WriteNcCW.WriteNcFile_test(rdd1, "/D:/tmp/afterRename/")
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("reName_AfterSplitCW", start, end, "flag=" + flag)
  }
}
