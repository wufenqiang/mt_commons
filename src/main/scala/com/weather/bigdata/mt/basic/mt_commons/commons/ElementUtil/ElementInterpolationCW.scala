package com.weather.bigdata.mt.basic.mt_commons.commons.ElementUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.VariableOperationCW
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.Constant
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ElementInterpolationCW {
  private val VO = VariableOperationCW

  def eleInterpolation(rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.eleInterpolation(scidata))
  }

  def main(args: Array[String]): Unit = {
    val infile = "/D:/Data/forecast/Output/NMC/J50_WINSD.nc/"
    val outfile = "/D:/Data/forecast/Output/NMC/test1/"
    var scidata1 = ReadNcCW.ReadNcScidata(infile)
    scidata1 = AttributesOperationCW.updateTimeStep(scidata1, 12.0d)
    val scidata2 = this.eleInterpolation(scidata1)

    WriteNcCW.WriteNcFile_test(scidata2, outfile)
  }

  def eleInterpolation(scidata: SciDatasetCW): SciDatasetCW = {
    val fcType = AttributesOperationCW.getFcType(scidata)
    val timeStep = AttributesOperationCW.gettimeStep(scidata)
    if (Constant.WIND.equals(fcType) && timeStep == 12.0d) {
      //      val startTime = System.currentTimeMillis()

      val idName = AttributesOperationCW.getIDName(scidata)
      val times = ArrayOperation.ArithmeticArray(12.0d, 240.0d, 12.0d)
      val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)

      val timeV = scidata.variables.apply(timeName)
      val timesV = VariableOperationCW.creatVar(times, timeV)
      val heightV = scidata.variables.apply(heightName)
      val latV = scidata.variables.apply(latName)
      val lonV = scidata.variables.apply(lonName)

      val windSV0: VariableCW = scidata.apply(WeatherElementCW.windSName)

      val windsIndexdata = this.WhichMaxIndexs(windSV0, timesV, timeV, heightV, latV, lonV)
      val windsV = scidata.variables.apply(WeatherElementCW.windSName)
      val winddV = scidata.variables.apply(WeatherElementCW.windDName)

      val windSdata = Array.ofDim[Double](windsIndexdata.length)
      val windDdata = Array.ofDim[Double](windsIndexdata.length)

      for (i <- 0 until windSdata.length - 1) {
        windSdata(i) = windsV.dataDouble(windsIndexdata(i))
      }
      for (i <- 0 until windDdata.length - 1) {
        windDdata(i) = winddV.dataDouble(windsIndexdata(i))
      }


      val timeLen = timesV.getLen()
      val heightLen = heightV.getLen()
      val latLen = latV.getLen()
      val lonLen = lonV.getLen()
      val windSV = VariableOperationCW.creatVars4(windSdata, WeatherElementCW.windSName, timeName, heightName, latName, lonName, timeLen, heightLen, latLen, lonLen)
      val windDV = VariableOperationCW.creatVars4(windDdata, WeatherElementCW.windDName, timeName, heightName, latName, lonName, timeLen, heightLen, latLen, lonLen)

      val vars = new ArrayBuffer[VariableCW]
      vars += timesV += heightV += latV += lonV += windSV += windDV

      val attr = scidata.attributes
      val datasetName: String = scidata.datasetName
      //      val varHash: ArrayBuffer[(String,Variable)]=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))
      val attrnew: mutable.HashMap[String, String] = AttributesOperationCW.addTrace(attr, "eleInterpolation")


      val scidata0 = new SciDatasetCW(vars, attrnew, datasetName)


      //--------------------日志采集----------------------------------
      //      val fileList = ""
      //      var basicRemark: JSONObject = new JSONObject()
      //      basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
      //      basicRemark = AttributesOperationCW.addTimeStep(basicRemark, timeStep)
      //      basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
      //      basicRemark = AttributesOperationCW.addOtherMark(basicRemark, "eleInterpolation")
      //      val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
      //      val qualityFlagStr: String = "N"
      //      var stateRemark: JSONObject = new JSONObject()
      //      val stateDetailRemark: JSONObject = new JSONObject()
      //      SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
      //--------------------------------------------------------------

      scidata0
    } else {
      val msg = "eleInterpolation配置出错,fcType=" + fcType + ";timeStep=" + timeStep
      val e: Exception = new Exception(msg)
      e.printStackTrace()
      scidata
    }
  }

  private def WhichMaxIndexs(v: VariableCW, timesV: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW): Array[Int] = {
    //    val vdata = v.data()
    val time = timeV.dataDouble()
    val times = timesV.dataDouble()
    //原始数组长度
    val timeLen = timeV.getLen()
    val timesLen = timesV.getLen()

    val timeMax = time.max
    val timeMin = time.min

    val height = heightV.dataDouble()
    val lat = latV.dataDouble()
    val lon = lonV.dataDouble()


    val dataInt: Array[Int] = new Array[Int](times.length * height.length * lat.length * lon.length)

    val heightlatlonLen = height.length * lat.length * lon.length


    var index, index0, index1 = 0
    val dtime: Double = time(1) - time(0)
    val dtimes: Double = times(1) - times(0)
    val dtimeNum: Int = (dtimes / dtime).toInt
    for (i <- 0 until timesLen - 1) {
      val x01Index = ArrayOperation.HashMapSearch(time, times(i))
      index1 = x01Index * heightlatlonLen
      for (j <- 0 until heightlatlonLen - 1) {
        var max: Double = -9999d
        var indexMax: Int = -1
        for (k <- 0 until dtimeNum - 1) {
          index0 = index1 - k * heightlatlonLen
          if (max < v.dataDouble(index0)) {
            indexMax = index0
          }
          max = math.max(max, v.dataDouble(index0))
        }
        dataInt(index) = indexMax
        index += 1
        index1 += 1
      }
    }


    dataInt
  }
}
