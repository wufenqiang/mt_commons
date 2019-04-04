package com.weather.bigdata.mt.basic.mt_commons.commons.UnitUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Offset.DataOffsetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

object NMCoffsetCW {
  def dataoffsetNMC (rdd0: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd0.map(sciData => this.dataoffsetNMC(sciData))
  }
  def dataoffsetNMC (scidata: SciDatasetCW): SciDatasetCW = {
    val startTime = System.currentTimeMillis( )
    val scidata0: SciDatasetCW = {
      val fcType = AttributesOperationCW.getFcType(scidata)
      val idName = AttributesOperationCW.getIDName(scidata)
      if (fcType.equals(Constant.TEM) || fcType.equals(Constant.PRS)) {
        val dataName: String = PropertiesUtil.getrElement_rename(fcType).last
        val (radio, offset) = PropertiesUtil.getFcTypeRadioOffset(fcType)
        DataOffsetCW.dataoffset(scidata, dataName, radio, offset)
      } else {
        scidata
      }
    }

    val endTime = System.currentTimeMillis( )

    //--------------------日志采集----------------------------------
    //    val fileList = ""
    //    var basicRemark: JSONObject = new JSONObject( )
    //    basicRemark = AttributesOperation.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperation.addIDName(basicRemark, idName)
    //    basicRemark = AttributesOperation.addOtherMark(basicRemark, "(" + fcType + ")unitChange")
    //    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject( )
    //    val stateDetailRemark: JSONObject = new JSONObject( )
    //    SendMsg.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
    //--------------------------------------------------------------


    //    val msg=ws.showDateStr("("+fcType+")unitChange:",startTime,endTime)
    //    println(msg)

    //    WeatherShowtime.showDateStrOut1("unitChange:",startTime,endTime,"fcType="+fcType)

    scidata0
  }
  def main(args:Array[String]): Unit ={
    val start = System.currentTimeMillis( )
    val ssc = ContextUtil.getSciSparkContextCW( )
    //    val ssc=ContextUtil.getSciSparkContext()
    val fileName = "/D:/tmp/afterRename/"
    val rdd0:RDD[SciDatasetCW] = ssc.sciDatasets(fileName)
    val rdd1=this.dataoffsetNMC(rdd0)
    val flag=WriteNcCW.WriteNcFile_test(rdd1, "/D:/tmp/afterUnit/")
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("NMCoffsetCW", start, end,"flag="+flag)
  }
}
