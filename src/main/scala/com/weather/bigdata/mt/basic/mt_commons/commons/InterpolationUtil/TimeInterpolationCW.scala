package com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{VariableOperationCW, ucarma2ArrayOperationCW}
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.SplitMatchInfo
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TimeInterpolationCW extends Serializable {



  /**
    * @param rdd
    * @param dataNames_alTypetimeIAs ：(dataName,alTypetimeIAs)递增Line,Line_Percent,Fill,Difference,( 均匀Ave,Sum,Min,Max )
    * @param times
    * @return
    */
  def timeIA (rdd: RDD[SciDatasetCW], varsName: (String, String, String, String), dataNames_alTypetimeIAs: Array[(String, String)], times: Array[Double]): RDD[SciDatasetCW] = {
    if (times == null) {
      rdd
    } else {
      val interRdd: RDD[SciDatasetCW] = {
        rdd.map(
          sciData => {
            this.timeIA(sciData, varsName, dataNames_alTypetimeIAs, times)
          }
        )

      }
      interRdd
    }
  }

  def timeIA (rdd: RDD[SciDatasetCW], varsName: (String, String, String, String), dataNames_alTypetimeIAs: Array[(String, String)], times: Array[Float]): RDD[SciDatasetCW] = {
    this.timeIA(rdd, varsName, dataNames_alTypetimeIAs, times.map(f => f.toDouble))
  }

  def timeIA (scidata: SciDatasetCW, varsName: (String, String, String, String), dataNames_alTypetimeIAs: Array[(String, String)], times: Array[Float]): SciDatasetCW = {
    this.timeIA(scidata, varsName, dataNames_alTypetimeIAs, times.map(f => f.toDouble))
  }

  def timeIA (scidata: SciDatasetCW, varsName: (String, String, String, String), dataNames_alTypetimeIAs: Array[(String, String)], times: Array[Double]): SciDatasetCW = {

    val result = if (times == null || times.isEmpty) {
      scidata
    } else {
            val startTime = System.currentTimeMillis( )

      //      val idName = AttributesOperationCW.getIDName(scidata)
      val (timeName, heightName, latName, lonName) = varsName


      val timeV = scidata.apply(timeName)
      val heightV = scidata.apply(heightName)
      val latV = scidata.apply(latName)
      val lonV = scidata.apply(lonName)

      val timesV = VariableOperationCW.creatVar(times, timeV)

      val mainsV: ArrayBuffer[VariableCW] = this.timeIA_GetMainV(scidata, timesV, timeV, heightV, latV, lonV, dataNames_alTypetimeIAs)

      val scidata0 = new SciDatasetCW(mainsV, scidata.attributes, scidata.datasetName)

      //      dataNames_alTypetimeIAs.foreach(dataName_alTypetimeIA => scidata.variables.remove(dataName_alTypetimeIA._1))
      //      mainsV.foreach(mainV => scidata.update(mainV.name, mainV))


      //      scidata.update(timeName, timesV)


      val alType_timeIA: String = {
        var alType: String = ""
        val timesMin = times.min
        val timesMax = times.max
        for (i <- 0 to dataNames_alTypetimeIAs.length - 1) {
          alType += dataNames_alTypetimeIAs(i)._1 + "(" + dataNames_alTypetimeIAs(i)._2 + ":" + timesMin + "-" + timesMax + ")"
        }
        alType
      }
      AttributesOperationCW.addTrace(scidata0, "timeIA[" + alType_timeIA + "]")

      //添加timeStep
      val timeStep: Double = (times.last - times.head) / (times.length - 1)
      AttributesOperationCW.updateTimeStep(scidata0, timeStep)


      //      //--------------------日志采集----------------------------------
      //      val endTime = System.currentTimeMillis( )
      //      val fileList = ""
      //      var basicRemark: JSONObject = new JSONObject( )
      //      basicRemark = AttributesOperationCW.addOtherMark(basicRemark, alType_timeIA)
      //      basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
      //      basicRemark = AttributesOperationCW.addbasicProcess(basicRemark, "时间插值")
      //      val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata)
      //      val qualityFlagStr: String = "N"
      //      var stateRemark: JSONObject = new JSONObject( )
      //      val stateDetailRemark: JSONObject = new JSONObject( )
      //      SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
      //      //--------------------------------------------------------------
      val endTime = System.currentTimeMillis( )
      WeatherShowtime.showDateStrOut1("TimeInterpolationCW("+scidata0.datasetName+")", startTime, endTime)

      scidata0
    }

    result
  }
  /*private def GetInterpolationVars(sciData: SciDataset, timeName:String,heightName:String,latName:String,lonName:String,times:Array[Double]): ArrayBuffer[Variable] = {
    val fileVals=VO.getVars(sciData,Array(heightName,latName,lonName))
    val timeV=VO.getVar(sciData,timeName)
    val timesV=VO.OneDimVar(times,timeV.name,timeV)

    fileVals+=timesV

    fileVals
  }*/

  def getnewdataName(dataNames_alTypetimeIAs: Array[(String, String)]): Array[String] = {
    dataNames_alTypetimeIAs.map(dataName_alTypetimeIA => this.getnewdataName(dataName_alTypetimeIA._1, dataName_alTypetimeIA._2, this.getPlusflag(dataNames_alTypetimeIAs)))
  }

  private def getPlusflag (dataNames_alTypetimeIAs: Array[(String, String)]): Boolean = {
    val dataNameSet: mutable.HashSet[String] = new mutable.HashSet[String]( )
    val alTypeSet: mutable.HashSet[String] = new mutable.HashSet[String]( )
    dataNames_alTypetimeIAs.foreach(
      dataName_alTypetimeIA => {
        val dn: String = dataName_alTypetimeIA._1
        val al: String = dataName_alTypetimeIA._2
        dataNameSet.add(dn)
        alTypeSet.add(al)
      }
    )
    if (dataNameSet.size == 1 && alTypeSet.size != 1) {
      true
    } else if (dataNameSet.size == alTypeSet.size && dataNameSet.size != dataNames_alTypetimeIAs.length) {
      true
    } else {
      false
    }
  }

  def getnewdataName(dataName: String, alType_timeIA: String, dataNamePlusflag: Boolean): String = {
    if (dataNamePlusflag) {
      /*dataName+"_"+alType_timeIA*/
      SplitMatchInfo.oldVar2newVar(dataName, alType_timeIA)
    } else {
      dataName
    }
  }

  private def timeIA_GetMainV(sciData: SciDatasetCW, timesV: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, dataNames_alTypetimeIAs: Array[(String, String)]): ArrayBuffer[VariableCW] = {

    val dataNamePlusflag = this.getPlusflag(dataNames_alTypetimeIAs)

    val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]
    vals += timesV += heightV += latV += lonV

    AttributesOperationCW.clearDataNames(sciData)
    dataNames_alTypetimeIAs.foreach(dataName_alTypetimeIA => {
      val dataName: String = dataName_alTypetimeIA._1
      val alType_timeIA = dataName_alTypetimeIA._2
      val msg = "dataName=" + dataName + ";alType_timeIA=" + alType_timeIA
      println(msg)

      val v = sciData.apply(dataName)
      //      AttributesOperationCW.addDataNames()
      val newdataName = this.getnewdataName(dataName, alType_timeIA, dataNamePlusflag)
      AttributesOperationCW.addDataNames(sciData, newdataName)
      vals += this.TimeMainDataVariable(v, timesV, timeV, heightV, latV, lonV, alType_timeIA, dataNamePlusflag)
    })

    vals
  }

  private def TimeMainDataVariable(v: VariableCW, timesV: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, alType_timeIA: String, dataNamePlusflag: Boolean): VariableCW = {

    val time: Array[Float] = timeV.dataFloat
    val times: Array[Float] = timesV.dataFloat

    val timesLen = timesV.getLen()

    val timeMin = time.min

    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    val shape = Array(timesLen, heightLen, latLen, lonLen)

    val dataTypeStr: DataType = DataType.getType(v.dataType)
    val data: ucar.ma2.Array = ucar.ma2.Array.factory(dataTypeStr, shape)

    val heightlatlonLen = heightLen * latLen * lonLen


    if (Constant.LineKey.equals(alType_timeIA)) {
      var index, index0, index1: Int = 0
      val x01Indexs = ArrayOperation.betweenIndex_sequence(time, times)
      for (i <- 0 to timesLen - 1) {
        val x = times(i)
        val x0 = time(x01Indexs(i)(0))
        val x1 = time(x01Indexs(i)(1))
        index0 = x01Indexs(i)(0) * heightlatlonLen
        index1 = x01Indexs(i)(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          val y0 = v.dataFloat(index0)
          val y1 = v.dataFloat(index1)
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Line(x0, x1, y0, y1, x))
          index += 1
          index0 += 1
          index1 += 1
        }
      }
    } else if (Constant.Line_PercentKey.equals(alType_timeIA)) {
      var index, index0, index1: Int = 0
      val x01Indexs = ArrayOperation.betweenIndex_sequence(time, times)
      for (i <- 0 to timesLen - 1) {
        val x = times(i)
        val x0 = time(x01Indexs(i)(0))
        val x1 = time(x01Indexs(i)(1))
        index0 = x01Indexs(i)(0) * heightlatlonLen
        index1 = x01Indexs(i)(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          val y0 = v.dataFloat(index0)
          val y1 = v.dataFloat(index1)
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Line_Percent(x0, x1, y0, y1, x))
          index += 1
          index0 += 1
          index1 += 1
        }
      }
    } else if (Constant.Line_Between0Key.equals(alType_timeIA)) {
      val ValueMin: Float = 300.0f
      val ValueMax: Float = 1080.0f
      var index, index0, index1: Int = 0
      val x01Indexs = ArrayOperation.betweenIndex_sequence(time, times)
      for (i <- 0 to timesLen - 1) {
        val x = times(i)
        val x0 = time(x01Indexs(i)(0))
        val x1 = time(x01Indexs(i)(1))
        index0 = x01Indexs(i)(0) * heightlatlonLen
        index1 = x01Indexs(i)(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          val y0 = v.dataFloat(index0)
          val y1 = v.dataFloat(index1)
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Line_Between(x0, x1, y0, y1, x, ValueMin, ValueMax))
          index += 1
          index0 += 1
          index1 += 1
        }
      }
    } else if (Constant.Line_Between1Key.equals(alType_timeIA)) {
      val ValueMin: Float = 0.0f

      var index, index0, index1: Int = 0
      val x01Indexs = ArrayOperation.betweenIndex_sequence(time, times)
      for (i <- 0 to timesLen - 1) {
        val x = times(i)
        val x0 = time(x01Indexs(i)(0))
        val x1 = time(x01Indexs(i)(1))
        index0 = x01Indexs(i)(0) * heightlatlonLen
        index1 = x01Indexs(i)(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          val y0 = v.dataFloat(index0)
          val y1 = v.dataFloat(index1)
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Line_Min(x0, x1, y0, y1, x, ValueMin))
          index += 1
          index0 += 1
          index1 += 1
        }
      }
    } else if (Constant.DifferenceKey.equals(alType_timeIA)) {
      var index: Int = 0
      val x01Indexs: Array[Array[Int]] = ArrayOperation.betweenIndex_sequence(time, times)

      for (i <- 0 to timesLen - 1) {
        val x01Index: Array[Int] = x01Indexs(i)
        val x = times(i)
        val (x0, x1) = (time(x01Indexs(i)(0)), time(x01Indexs(i)(1)))

        var index1 = {
          if (x <= timeMin) {
            x01Index(0) * heightlatlonLen
          } else {
            x01Index(1) * heightlatlonLen
          }
        }
        for (j <- 0 to heightlatlonLen - 1) {
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Difference(x1 - x0, v.dataFloat(index1)))
          index += 1
          index1 += 1
        }
      }
    } else if (Constant.AveKey.equals(alType_timeIA)) {
      var index, index0, index1 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x01Index = ArrayOperation.betweenIndex_sequence(time, times(i))
        index1 = x01Index(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          var sum: Float = 0f
          for (k <- 0 to dtimeNum - 1) {
            index0 = index1 - k * heightlatlonLen
            sum = sum + v.dataFloat(index0)
          }
          ucarma2ArrayOperationCW.setObject(data, index, Algorithm1D.Ave(sum, dtimeNum))
          index += 1
          index1 += 1
        }
      }
    } else if (Constant.SumKey.equals(alType_timeIA)) {
      var index, index0, index1 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x01Index = ArrayOperation.HashMapSearch(time, times(i))
        index1 = x01Index * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          var sum: Double = 0d
          for (k <- 0 to dtimeNum - 1) {
            index0 = index1 - k * heightlatlonLen
            sum = sum + v.dataDouble(index0)
          }
          ucarma2ArrayOperationCW.setObject(data, index, sum)
          index += 1
          index1 += 1
        }
      }
    } else if (Constant.MinKey.equals(alType_timeIA)) {
      var index, index0, index1 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x01Index = ArrayOperation.HashMapSearch(time, times(i))
        index1 = x01Index * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          var min: Double = 9999d
          for (k <- 0 to dtimeNum - 1) {
            index0 = index1 - k * heightlatlonLen
            min = math.min(min, v.dataDouble(index0))
          }
          ucarma2ArrayOperationCW.setObject(data, index, min)
          index += 1
          index1 += 1
        }
      }
    } else if (Constant.MaxKey.equals(alType_timeIA)) {
      var index, index0, index1 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x01Index = ArrayOperation.HashMapSearch(time, times(i))
        index1 = x01Index * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          var max: Double = -9999d
          for (k <- 0 to dtimeNum - 1) {
            index0 = index1 - k * heightlatlonLen
            max = math.max(max, v.dataDouble(index0))
          }
          ucarma2ArrayOperationCW.setObject(data, index, max)
          index += 1
          index1 += 1
        }
      }
    } else if (Constant.FillKey.equals(alType_timeIA)) {
      var index, index0 = 0
      for (i <- 0 to timesLen - 1) {
        val xIndex = ArrayOperation.nearIndex_sequence(time, times(i))
        index0 = xIndex * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          ucarma2ArrayOperationCW.setObject(data, index, v.dataObject(index0))
          index += 1
          index0 += 1
        }
      }
    } else if (Constant.WhichFirstKey.equals(alType_timeIA)) {
      var index, index1, index2 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x12Index = ArrayOperation.betweenIndex_sequence(time, times(i))
        index2 = x12Index(0) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          ucarma2ArrayOperationCW.setObject(data, index, v.dataObject(index2))
          index += 1
          index2 += 1
        }
      }
    } else if (Constant.WhichSecondKey.equals(alType_timeIA)) {
      var index, index1, index2 = 0
      val dtime: Double = time(1) - time(0)
      val dtimes: Double = times(1) - times(0)
      val dtimeNum: Int = (dtimes / dtime).toInt
      for (i <- 0 to timesLen - 1) {
        val x12Index = ArrayOperation.betweenIndex_sequence(time, times(i))
        index2 = x12Index(1) * heightlatlonLen
        for (j <- 0 to heightlatlonLen - 1) {
          ucarma2ArrayOperationCW.setObject(data, index, v.dataObject(index2))
          index += 1
          index2 += 1
        }
      }
    } else {
      val msg = "timeIA中," + alType_timeIA + "算法类型不存在!"
      val e: Exception = new Exception(msg)
      e.printStackTrace( )
    }
    val dataName = v.name
    val newdataName = this.getnewdataName(dataName, alType_timeIA, dataNamePlusflag)
    var att = v.attributes
    att = AttributesOperationCW.addTrace(att, "timeIA[" + alType_timeIA + "]")

    VariableOperationCW.creatVars4(data, att, newdataName, timeV.name, heightV.name, latV.name, lonV.name)
  }


  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )
    val ssc = ContextUtil.getSciSparkContextCW( )
    //    val ssc=ContextUtil.getSciSparkContext()
    val fcType = "TEM"
    val timeStep = 1.0d
    val InputfileName = "/D:/tmp/afterlatlon/"
    val OutputfileName = "/D:/tmp/aftertime/" + timeStep.toInt + "h/"
    val rdd0 = ssc.sciDatasets(InputfileName)
    val varsName: (String, String, String, String) = ("time", "height_above_ground", "lat", "lon")
    //    val rdd1=AttributesOperationCW.addVarName(rdd0,varsName)

    val dataNames_timeIA: Array[(String, String)] = PropertiesUtil.getdataNames_alTypetimeIAs_from3h(fcType, timeStep)
    val times = PropertiesUtil.gettimes(timeStep)
    val rdd1 = this.timeIA(rdd0, varsName, dataNames_timeIA, times)
    WriteNcCW.WriteNcFile_test(rdd1, OutputfileName)
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("TimeInterpolationCW", start, end)
  }
}
