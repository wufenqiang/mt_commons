package com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{IndexOperation, VariableOperationCW, ucarma2ArrayOperationCW}
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * 空间插值工具类
  * created by robin on 2018/1/17
  * created by wufenqiang
  */

//extends Serializable
object LatLonInterpolationCW extends Serializable {

  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )
    val ssc = ContextUtil.getSciSparkContextCW
    val InputfileName = "/D:/tmp/afterUnit/"
    val OutputfileName = "/D:/tmp/afterlatlon/"
    val fcRdd_unSplit: RDD[SciDatasetCW] = ssc.sciDatasets(InputfileName)
    val (timeName: String, heightName: String, latName: String, lonName: String) = PropertiesUtil.getVarName()
    val frontRdds: RDD[SciDatasetCW] = this.Interpolation(fcRdd_unSplit, 0.05, 0.01, timeName, heightName, latName, lonName, Array("TEM"))
    WriteNcCW.WriteNcFile_test(frontRdds, OutputfileName)
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("LatLonInterpolationCW", start, end)
  }

  def Interpolation (rdd: RDD[SciDatasetCW], beginStep: Double, endStep: Double, timeName: String, heightName: String, latName: String, lonName: String, dataNames: Array[String]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.Interpolation(scidata, endStep, timeName, heightName, latName, lonName, dataNames))
  }

  def Interpolation(scidata: SciDatasetCW, endStep: Double, timeName: String, heightName: String, latName: String, lonName: String, dataNames: Array[String]): SciDatasetCW = {
    val start = System.currentTimeMillis()
    val scidata0 = this.latlon_GetMainV(scidata, endStep, timeName, heightName, latName, lonName, dataNames)
    val end = System.currentTimeMillis()
    WeatherShowtime.showDateStrOut1("LatLonInterpolationCW("+scidata0.datasetName+")", start, end)

    //    val fcType = AttributesOperationCW.getFcType(scidata0)
    //    val idName = AttributesOperationCW.getIDName(scidata0)

    //    //--------------------日志采集----------------------------------

    //    val fileList = ""
    //    var basicRemark: JSONObject = new JSONObject( )
    //    basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
    //    basicRemark = AttributesOperationCW.addbasicProcess(basicRemark, "空间插值")
    //    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject( )
    //    stateRemark = AttributesOperationCW.addOtherMark(stateRemark, "Interpolation[" + beginStep + "=>" + endStep + "]")
    //    val stateDetailRemark: JSONObject = new JSONObject( )
    //    SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
    //    //--------------------------------------------------------------


    //    WeatherShowtime.showDateStrOut1("InterpolationUtil.Interpolation", startTime, endTime)

    scidata0
  }

  private def latlon_GetMainV(scidata: SciDatasetCW, endStep: Double, timeName: String, heightName: String, latName: String, lonName: String, dataNames: Array[String]): SciDatasetCW = {
    val datasetName: String = scidata.datasetName
    val mainV: ArrayBuffer[VariableCW] = {
      val newvals = new ArrayBuffer[VariableCW]()

      val oldvals: mutable.HashMap[String, VariableCW] = scidata.variables

      println("=====Job 差值 --->main variable")
      val timeV: VariableCW = oldvals.apply(timeName)
      val heightV: VariableCW = oldvals.apply(heightName)
      val latV0: VariableCW = oldvals.apply(latName)
      val lonV0: VariableCW = oldvals.apply(lonName)

      val lat0: Array[Float] = NumOperation.K2dec(latV0.dataFloat())
      val lon0: Array[Float] = NumOperation.K2dec(lonV0.dataFloat())

      val lat1: Array[Float] = NumOperation.K2dec(ArrayOperation.ArithmeticArray(NumOperation.K2dec(lat0.head), NumOperation.K2dec(lat0.last), endStep.toFloat))
      val latV1: VariableCW = VariableOperationCW.creatVar(lat1, latV0)
      val lon1: Array[Float] = NumOperation.K2dec(ArrayOperation.ArithmeticArray(NumOperation.K2dec(lon0.head), NumOperation.K2dec(lon0.last), endStep.toFloat))
      val lonV1: VariableCW = VariableOperationCW.creatVar(lon1, lonV0)

      val timeLen = timeV.getLen( )
      val heightLen = heightV.getLen( )

      val lat0Len = lat0.length
      val lon0Len = lon0.length
      val lat1Len = lat1.length
      val lon1Len = lon1.length

      val latIndexss = ArrayOperation.betweenIndex_sequence1(lat0, lat1)
      val lonIndexss = ArrayOperation.betweenIndex_sequence1(lon0, lon1)

      val shape = Array(timeLen, heightLen, lat1Len, lon1Len)

      dataNames.foreach(dataName => {
        val v0 = oldvals.apply(dataName)
        val array1 = ucar.ma2.Array.factory(DataType.getType(v0.dataType), shape)

        for (k <- 0 to lat1Len - 1) {
          val latValue: Float = lat1(k)
          val latIndexsHead: Int = latIndexss(k)._1
          val latIndexsLast: Int = latIndexss(k)._2
          val x0: Float = lat0(latIndexsHead)
          val x1: Float = lat0(latIndexsLast)
          for (l <- 0 to lon1Len - 1) {
            val lonValue: Float = lon1(l)
            val lonIndexsHead = lonIndexss(l)._1
            val lonIndexsLast = lonIndexss(l)._2
            val y0 = lon0(lonIndexsHead)
            val y1 = lon0(lonIndexsLast)
            for (i <- 0 to timeLen - 1) {
              for (j <- 0 to heightLen - 1) {
                val Q00Index: Int = IndexOperation.getIndex4_thlatlon(i, j, latIndexsHead, lonIndexsHead, heightLen, lat0Len, lon0Len)
                val Q01Index: Int = IndexOperation.getIndex4_thlatlon(i, j, latIndexsHead, lonIndexsLast, heightLen, lat0Len, lon0Len)
                val Q10Index: Int = IndexOperation.getIndex4_thlatlon(i, j, latIndexsLast, lonIndexsHead, heightLen, lat0Len, lon0Len)
                val Q11Index: Int = IndexOperation.getIndex4_thlatlon(i, j, latIndexsLast, lonIndexsLast, heightLen, lat0Len, lon0Len)
                val Q00: Float = v0.dataFloat(Q00Index)
                val Q01: Float = v0.dataFloat(Q01Index)
                val Q10: Float = v0.dataFloat(Q10Index)
                val Q11: Float = v0.dataFloat(Q11Index)
                val value: Float = Algorithm2D.doubleLine(Q00, Q01, Q10, Q11, x0, x1, y0, y1, latValue, lonValue)
                val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, lat1Len, lon1Len)
                ucarma2ArrayOperationCW.setFloat(array1, index, value)
              }
            }
          }
        }
        newvals += VariableOperationCW.creatVars4(array1, v0.attributes, dataName, timeName, heightName, latName, lonName)
      })


      newvals.+=(timeV).+=(heightV).+=(latV1).+=(lonV1)

      newvals

    }

    val scidata0 = new SciDatasetCW(mainV, scidata.attributes, datasetName)
    AttributesOperationCW.addTrace(scidata0, "InterpolationCW[" + endStep + "]")

    scidata0
  }

  /*private def getLatLonVariable (v: VariableCW, endStep0: Double): VariableCW = {
    //    val vdata=v.dataDouble()
    val oldlen = v.getLen( )
    val head: Double = NumOperation.K2dec(v.dataDouble(0))
    val last: Double = NumOperation.K2dec(v.dataDouble(oldlen - 1))
    val endStep = NumOperation.K2dec(endStep0)
    val data: Array[Double] = ArrayOperation.ArithmeticArray(head, last, endStep)
    val newLen = data.length
    println(v.name + "------oldLen=" + oldlen + ",newLen=" + newLen + ",(head=" + head + ",last=" + last + ",endStep=" + endStep + ")----------")

    VariableOperationCW.creatVar(data, v)
  }*/

}
