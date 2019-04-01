package com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Offset

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object DataOffsetCW {
//  def dataoffsetNMC (rdd0: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
//    rdd0.map(sciData => this.dataoffsetNMC(sciData))
//  }
//  def dataoffsetNMC (scidata: SciDatasetCW): SciDatasetCW = {
//    val startTime = System.currentTimeMillis( )
//    val scidata0: SciDatasetCW = {
//      val fcType = AttributesOperationCW.getFcType(scidata)
//      val idName = AttributesOperationCW.getIDName(scidata)
//      if (fcType.equals(Constant.TEM) || fcType.equals(Constant.PRS)) {
//        val dataName: String = PropertiesUtil.getrElement_rename(fcType).last
//        val (radio, offset) = PropertiesUtil.getFcTypeRadioOffset(fcType)
//        this.dataoffset(scidata, dataName, radio, offset)
//      } else {
//        scidata
//      }
//    }
//
//    val endTime = System.currentTimeMillis( )
//
//    //--------------------日志采集----------------------------------
////    val fileList = ""
////    var basicRemark: JSONObject = new JSONObject( )
////    basicRemark = AttributesOperation.addFcType(basicRemark, fcType)
////    basicRemark = AttributesOperation.addIDName(basicRemark, idName)
////    basicRemark = AttributesOperation.addOtherMark(basicRemark, "(" + fcType + ")unitChange")
////    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
////    val qualityFlagStr: String = "N"
////    var stateRemark: JSONObject = new JSONObject( )
////    val stateDetailRemark: JSONObject = new JSONObject( )
////    SendMsg.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
//    //--------------------------------------------------------------
//
//
//    //    val msg=ws.showDateStr("("+fcType+")unitChange:",startTime,endTime)
//    //    println(msg)
//
//    //    WeatherShowtime.showDateStrOut1("unitChange:",startTime,endTime,"fcType="+fcType)
//
//    scidata0
//  }

  def dataoffset (rdd0: RDD[SciDatasetCW], dataNames: Array[String], radio: Double, offset: Double): RDD[SciDatasetCW] = {
    rdd0.map(scidata0 => {
      this.dataoffset(scidata0, dataNames, radio, offset)
    })
  }
  def dataoffset (scidata: SciDatasetCW, dataNames: Array[String], radio: Double, offset: Double): SciDatasetCW = {
    val startTime = System.currentTimeMillis( )
    //    val fcType=AttributesOperationCW.getFcType(scidata)
    val idName = AttributesOperationCW.getIDName(scidata)
    val scidata0: SciDatasetCW = {
      //      val dataName:String=PropertiesUtil.getrElement_rename(fcType).last

      if (radio == 1.0d && offset == 0.0d) {
        scidata
      } else {
        val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()
        val Vars: ArrayBuffer[VariableCW] = {
          ArrayBuffer(timeName, heightName, latName, lonName).map(f => scidata.apply(f))
        }

        dataNames.foreach(dataName => {
          val dataV = scidata.apply(dataName)
          Vars += this.dataoffsetV(dataV, radio, offset)
        })

        val idName = AttributesOperationCW.getIDName(scidata)
        //        val varHash = VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,Vars,(timeName,heightName,latName,lonName))
        val dataNameStr: String = dataNames.reduce((x, y) => (x + "," + y))
        val attrs_new = AttributesOperationCW.addTrace(scidata.attributes, "DataOffset(" + dataNameStr + ",radio=" + radio + ",offset=" + offset + ")")
        new SciDatasetCW(Vars, attrs_new, scidata.datasetName)
      }
    }

    val endTime = System.currentTimeMillis( )

    WeatherShowtime.showDateStrOut1("unitChange:", startTime, endTime, "dataNames=" + dataNames.reduce((x, y) => "(" + x + "," + y + ")") + ",radio=" + radio + ",offset=" + offset)

    scidata0
  }
  def dataoffset (scidata: SciDatasetCW, dataName: String, radio: Double, offset: Double): SciDatasetCW = {
    this.dataoffset(scidata: SciDatasetCW, Array(dataName), radio: Double, offset: Double)
  }
  def dataoffset (rdd0: RDD[SciDatasetCW], dataName: String, radio: Double, offset: Double): RDD[SciDatasetCW] = {
    rdd0.map(scidata0 => {
      this.dataoffset(scidata0, Array(dataName), radio, offset)
    })
  }

  private def dataoffsetV (dataV: VariableCW, radio: Double, offset: Double): VariableCW = {

    val len = dataV.getLen()
    for (i <- 0 until len - 1) {
      dataV.setdata(i, this.onlydataoffset(dataV.dataFloat(i), radio, offset))
    }

    dataV.insertAttributes(("radio=", radio.toString))
    dataV.insertAttributes(("offset=", offset.toString))

    dataV
  }
  def onlydataoffset (elem: Double, radio: Double, offset: Double): Double = {
    radio * elem + offset
  }

  def main (args: Array[String]): Unit = {
    /*val rfileName="/D:/Data/forecast/Output/NMC/test/"
    val wfileName="/D:/Data/forecast/Output/NMC/test1/"
    val fcRdd_Split:RDD[SciDataset]=ReadFile.ReadNcRdd(rfileName)
    val rdd=DataOffset.dataoffset(fcRdd_Split)
    WriteFile.WriteNcFile(rdd,wfileName)*/
  }
}
