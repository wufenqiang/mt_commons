package com.weather.bigdata.mt.basic.mt_commons.commons.QualityControlUtil

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object PostControlCW {

  private val dT: Float = 5.0f

  def postControl (Rdd: RDD[SciDatasetCW], ExtremMap: Map[String, Map[String, Array[Float]]]): RDD[SciDatasetCW] = {
    Rdd.map(scidata => this.postControl(scidata, ExtremMap))
  }

  def main (args: Array[String]): Unit = {
    val start = System.currentTimeMillis( )
    val timeStep = 12.0d
    val input = "/D:/tmp/aftertime/" + timeStep.toInt + "h/"
    val output = "/D:/tmp/afterPostControl/" + timeStep.toInt + "h/"
    val sc = ContextUtil.getSciSparkContextCW( )
    val rdd0: RDD[SciDatasetCW] = sc.sciDatasets(input)
    val rdd1 = this.postControlFc(rdd0)
    WriteNcCW.WriteNcFile_test(rdd1, output)
    val end = System.currentTimeMillis
    WeatherShowtime.showDateStrOut1("PostControlCW", start, end)
  }

  def postControlFc (Rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    Rdd.map(scidata => this.postControlFc(scidata))
  }

  def postControlFc (scidata: SciDatasetCW): SciDatasetCW = {
    val ExtremMap = {
      val fcType = AttributesOperationCW.getFcType(scidata)
      if (fcType.equals(Constant.TEM)) {
        ReadFile.ReadTemExFile(PropertiesUtil.getTemExtremPath)
      } else {
        Map( ): Map[String, Map[String, Array[Float]]]
      }
    }
    this.postControl(scidata, ExtremMap)
  }

  def postControl (scidata: SciDatasetCW, ExtremMap: Map[String, Map[String, Array[Float]]]): SciDatasetCW = {
    this.postControl0(scidata, ExtremMap)
  }

  private def postControl0 (scidata: SciDatasetCW, ExtremMap: Map[String, Map[String, Array[Float]]]): SciDatasetCW = {
    val startTime = System.currentTimeMillis()

    val fcType = AttributesOperationCW.getFcType(scidata)
    val timeStep: Double = AttributesOperationCW.gettimeStep(scidata)
    val idName: String = AttributesOperationCW.getIDName(scidata)
    val fcdate: Date = AttributesOperationCW.getfcdate(scidata)

    val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)

    val dataNames = AttributesOperationCW.getDataNames(scidata)
    val varHash: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    val timeV = scidata.apply(timeName)
    val heigthV = scidata.apply(heightName)
    val latV = scidata.apply(latName)
    val lonV = scidata.apply(lonName)

    varHash.+=(timeV).+=(heigthV).+=(latV).+=(lonV)

    //    val lat = latV.dataDouble()
    //    val lon = lonV.dataDouble()

    //获取数据维度大小信息
    val timeLen = timeV.getLen()
    val heightLen = heigthV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()
    val length = timeLen * heightLen * latLen * lonLen

    val dims = List((timeName, timeLen), (heightName, heightLen), (latName, latLen), (lonName, lonLen))
    val shape = Array(timeLen, heightLen, latLen, lonLen)

    //获取拆分区域的边界点和时空分辨率
    //    val beginTim = scidata.apply(timeName).data().head
    //    val endTim = scidata.apply(timeName).data().last
    //    val timeStep = (endTim - beginTim) / (timeLen - 1)

    //    val beginLat = lat.head
    //    val endLat = lat.last
    //
    //    val beginLon = lon.head
    //    val endLon = lon.last

    //    val spaceStep = (endLon - beginLon) / (lonLen - 1)
    //    println(" latBegein + " + beginLat + " latEnd + " + endLat + " lonBegein + " + beginLon + " lonEnd +" + endLon)

    dataNames.foreach(dataName => {
      val DataV = scidata.apply(dataName)

      if (fcType.equals(Constant.TEM)) {
        //按极值质控
        /*        var tmax = this.ReadTemExtremeData(tmaxV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0,beginLat, endLat, beginLon, endLon, spaceStep,"tmax")
                var tmin = this.ReadTemExtremeData(tminV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0, beginLat, endLat, beginLon, endLon, spaceStep,"tmin")*/
        val month: Int = fcdate.getMonth + 1
        for (k <- 0 to latLen - 1) {
          val lat = latV.dataFloat(k)
          for (l <- 0 to lonLen - 1) {
            val lon = lonV.dataFloat(l)
            val (min_temp, max_temp) = this.ValidTEMData(month, ExtremMap, (lat, lon))
            for (i <- 0 to timeLen - 1) {
              for (j <- 0 to heightLen - 1) {
                val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                val v0 = DataV.dataFloat(index)
                if (v0 > max_temp) {
                  DataV.setdata(index, max_temp)
                } else if (v0 < min_temp) {
                  DataV.setdata(index, min_temp)
                }
              }
            }
          }
        }
        //        for (i <- 0 until length) {
        //          val lat = beginLat + spaceStep * (i % (heightLen * latLen * lonLen) / lonLen)
        //          val lon = beginLon + spaceStep * (i % (heightLen * latLen * lonLen) % lonLen)
        //          DataV.setdata(i, this.ValidTEMData(fcdate, ExtremMap, DataV.dataFloat(i), lat, lon))
        //        }
      } else {
        //按临界点质控
        val (max: Float, min: Float) = {
          if (fcType.equals(Constant.PRE)) {
            (800.0f, 0.0f)
          } else if (fcType.equals(Constant.WIND)) {
            if (dataName.equals(PropertiesUtil.WIND)) {
              (360.0f, 0.0f)
            } else {
              (80.0f, 0.0f)
            }
          } else if (fcType.equals(Constant.RHU) || fcType.equals(Constant.CLOUD)) {
            (100.0f, 0.0f)
          } else if (fcType.equals(Constant.PRS)) {
            (1080.0f, 300.0f)
          } else {
            (100.0f, 0.0f)
          }
        }
        if (fcType.equals(Constant.PRE) || fcType.equals(Constant.WIND)) {
          //降水和风速异常大的时候订正值为0
          for (i <- 0 until length) {
            if (DataV.dataFloat(i) > max || DataV.dataFloat(i) < min) {
              DataV.setdata(i, min)
            }
          }
        } else {
          //其他订正值为最大最小值
          for (i <- 0 until length) {
            if (DataV.dataFloat(i) > max) {
              DataV.setdata(i, max)
            } else if (DataV.dataFloat(i) < min) {
              DataV.setdata(i, min)
            }
          }
        }
      }
      varHash.+=(DataV)

    })


    val sciDatasetName = scidata.datasetName
    val attrs = AttributesOperationCW.addTrace(scidata.attributes, "PostControl")
    val sci = new SciDatasetCW(varHash, attrs, sciDatasetName)

    //--------------------日志采集----------------------------------
    val endTime = System.currentTimeMillis()
    //    val fileList = ""
    //    var basicRemark: JSONObject = new JSONObject()
    //    basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
    //    basicRemark = AttributesOperationCW.addOtherMark(basicRemark, "Fc")
    //    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject()
    //    stateRemark = AttributesOperationCW.addOtherMark(stateRemark, "postControl")
    //    val stateDetailRemark: JSONObject = new JSONObject()
    //    SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
    //--------------------------------------------------------------


    //    val End=System.currentTimeMillis()
    /*    val msg=ws.showDateStr("QualityControl.PostControl",startTime,endTime)
        println(msg)*/

    WeatherShowtime.showDateStrOut1("QualityControl.PostControl("+sci.datasetName+")", startTime, endTime)

    sci
  }

  //  private def ValidTEMData(fcdate: Date, areaMap: Map[String, Map[String, Array[Float]]], data: Float, lat: Double, lon: Double): Float = { //		int a = new BigDecimal(lat * 100).intValue();
  //
  //    val month = fcdate.getMonth + 1
  //    // println("预报日期：" + fcdate+" 需要提取" + month + "月极值数据")
  //    //    var checkedData = data
  //    val gridKey = getGridKey(lat, lon)
  //    if (!areaMap.contains(gridKey)) {
  //      if(data>50.0f){
  //        50.0f
  //      }else if(data < -50.0f){
  //        -50.0f
  //      }else{
  //        data
  //      }
  //    }else {
  //      val max_temp :Float= areaMap.get(gridKey).get("max_temp")(month - 1)
  //      val min_temp :Float= areaMap.get(gridKey).get("min_temp")(month - 1)
  //      if(data > max_temp + this.dT){
  //        max_temp
  //      }else if(data < min_temp - this.dT){
  //        min_temp
  //      }else{
  //        data
  //      }
  //    }
  //  }

  private def ValidTEMData (month: Int, areaMap: Map[String, Map[String, Array[Float]]], latlon: (Float, Float)): (Float, Float) = { //		int a = new BigDecimal(lat * 100).intValue();
    val gridKey = this.getGridKey(latlon._1, latlon._2)

    if (!areaMap.contains(gridKey)) {
      (-50.0f, 50.0f)
    } else {
      val max_temp: Float = areaMap.get(gridKey).get("max_temp")(month - 1)
      val min_temp: Float = areaMap.get(gridKey).get("min_temp")(month - 1)
      (min_temp - dT, max_temp + dT)
    }
  }

  private def ValidTempData_Before (areaMap: Map[String, Map[String, Array[Double]]], data: Double, lat: Double, lon: Double): Double = { //		int a = new BigDecimal(lat * 100).intValue();

    val now = new Date( )
    val month = now.getMonth + 1
    // println("当前日期：" + now+" 需要提取" + month + "月极值数据")
    var checkedData = data
    val gridKey = getGridKey(lat, lon)
    if (!areaMap.contains(gridKey)) {
      data match {
        case d if (d > 50) => checkedData = 50
        case d if (d < -50) => checkedData = -50
        case _ => checkedData = data
      }
    }

    else {
      var areaArray = areaMap.get(gridKey)
      var max_temp = areaMap.get(gridKey).get("max_temp")(month - 1)
      var min_temp = areaMap.get(gridKey).get("min_temp")(month - 1)
      data match {
        case d if (d > max_temp + 5) => checkedData = max_temp
        case d if (d < (min_temp - 5)) => checkedData = min_temp
        case _ => checkedData = data
      }
    }
    checkedData
  }

  private def getGridKey (lat: Double, lon: Double): String = {
    var latS = new String
    var lonS = new String
    if (lat >= 12 & lat < 16) latS = "D"
    else if (lat >= 16 & lat < 20) latS = "E"
    else if (lat >= 20 & lat < 24) latS = "F"
    else if (lat >= 24 & lat < 28) latS = "G"
    else if (lat >= 28 & lat < 32) latS = "H"
    else if (lat >= 32 & lat < 36) latS = "I"
    else if (lat >= 36 & lat < 40) latS = "J"
    else if (lat >= 40 & lat < 44) latS = "K"
    else if (lat >= 44 & lat < 48) latS = "L"
    else if (lat >= 48 & lat < 52) latS = "M"
    else if (lat >= 52 & lat < 56) latS = "N"
    else latS = "out"
    if (lon >= 72 & lon < 78) lonS = "43"
    else if (lon >= 78 & lon < 84) lonS = "44"
    else if (lon >= 84 & lon < 90) lonS = "45"
    else if (lon >= 90 & lon < 96) lonS = "46"
    else if (lon >= 96 & lon < 102) lonS = "47"
    else if (lon >= 102 & lon < 108) lonS = "48"
    else if (lon >= 108 & lon < 114) lonS = "49"
    else if (lon >= 114 & lon < 120) lonS = "50"
    else if (lon >= 120 & lon < 126) lonS = "51"
    else if (lon >= 126 & lon < 132) lonS = "52"
    else if (lon >= 132 & lon < 138) lonS = "53"
    else lonS = "out"
    val gridKey = latS + lonS
    gridKey
  }

  private def postControl_Before(scidata: SciDatasetCW, ExtremMap: Map[String, Map[String, Array[Double]]]): SciDatasetCW = {
    val startTime = System.currentTimeMillis()

    val fcType = AttributesOperationCW.getFcType(scidata)
    val timeStep: Double = AttributesOperationCW.gettimeStep(scidata)
    val idName: String = AttributesOperationCW.getIDName(scidata)

    val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)
    val dataNames = {
      if (Constant.WIND.equals(fcType) && timeStep == 12.0d) {
        Array(PropertiesUtil.WINS, PropertiesUtil.WIND)
      } else {
        PropertiesUtil.getrElement_rename(fcType, timeStep)
      }
    }
    //    var varHash:mutable.HashMap[String, Variable]=new mutable.HashMap[String, Variable]
    val varHash: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    val timeV = scidata.apply(timeName)
    val heigthV = scidata.apply(heightName)
    val latV = scidata.apply(latName)
    val lonV = scidata.apply(lonName)

    varHash.+=(timeV).+=(heigthV).+=(latV).+=(lonV)


    //获取数据维度大小信息
    val timeDims = timeV.getLen()
    val heightDims = heigthV.getLen()
    val latDims = latV.getLen()
    val lonDims = lonV.getLen()
    val length = timeDims * heightDims * latDims * lonDims
    val dims = List((timeName, timeDims), (heightName, heightDims), (latName, latDims), (lonName, lonDims))
    val shape = Array(timeDims, heightDims, latDims, lonDims)

    val time = timeV.dataDouble()
    val lat = latV.dataDouble()
    val lon = lonV.dataDouble()


    //获取拆分区域的边界点和时空分辨率
    val beginTim = time.head
    val endTim = time.last
    //    val timeStep = (endTim - beginTim) / (timeDims - 1)
    val beginLat = lat.head
    val endLat = lat.last
    val beginLon = lon.head
    val endLon = lon.last
    val spaceStep = (endLon - beginLon) / (lonDims - 1)
    println(" latBegein + " + beginLat + " latEnd + " + endLat + " lonBegein + " + beginLon + " lonEnd +" + endLon)
    //    val RevisedData = new Array[Double](length)

    /*dataNames.foreach(
      dataName=>{

        val Data = SciData.apply(dataName).data()

        if(fcType.equals(ConstantUtil.TEM)){//按极值质控
          /*        var tmax = this.ReadTemExtremeData(tmaxV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0,beginLat, endLat, beginLon, endLon, spaceStep,"tmax")
                  var tmin = this.ReadTemExtremeData(tminV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0, beginLat, endLat, beginLon, endLon, spaceStep,"tmin")*/

          for(i <- 0 until length) {
            val lat = beginLat + spaceStep* (i%(heightDims *latDims * lonDims)/lonDims)
            val lon = beginLon + spaceStep* (i%(heightDims *latDims * lonDims)%lonDims)
            RevisedData(i) = this.ValidTempData(ExtremMap ,Data(i), lat, lon)
          }
        }else{//按临界点质控
        val (max:Double,min:Double)={
          if(fcType.equals(ConstantUtil.PRE)){
            (800.0d,0.0d)
          }else if(fcType.equals(ConstantUtil.WIND)){
            (80.0d,0.0d)
          }else if(fcType.equals(ConstantUtil.RHU)||fcType.equals(ConstantUtil.CLOUD)){
            (100.0d,0.0d)
          }else if(fcType.equals(ConstantUtil.PRS)){
            (1080.0d,300.0d)
          }else{
            (100.0d,0.0d)
          }
        }


          for(i <- 0 until length) {
            if(Data(i)>max){
              if(fcType.equals(ConstantUtil.PRE)||fcType.equals(ConstantUtil.WIND)){//降水和风速异常大的时候订正值为0
                RevisedData(i)=min
              }else{
                RevisedData(i)=max
              }
            }else if(Data(i)<min){
              RevisedData(i)=min
            }else{
              RevisedData(i) = Data(i)
            }
          }
        }

        val revisedVar=new Variable(dataName, SciData.apply(dataName).dataType, new Nd4jTensor(RevisedData, shape), SciData.apply(dataName).attributes, dims)
        varHash = SciData.variables.map(p => {
          p._1 match {
            case i if i.equals(dataName) => (p._1, revisedVar)
            case _ => p
          }
        })
      })*/

    for (i <- 0 to dataNames.length - 1) {
      val dataName: String = dataNames(i)
      val DataV: VariableCW = scidata.apply(dataName)

      if (fcType.equals(Constant.TEM)) {
        //按极值质控
        /*        var tmax = this.ReadTemExtremeData(tmaxV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0,beginLat, endLat, beginLon, endLon, spaceStep,"tmax")
                var tmin = this.ReadTemExtremeData(tminV,ExlatDims, ExlonDims,ExtimeDims,lat0,lon0, beginLat, endLat, beginLon, endLon, spaceStep,"tmin")*/

        for (i <- 0 until length) {
          val lat = beginLat + spaceStep * (i % (heightDims * latDims * lonDims) / lonDims)
          val lon = beginLon + spaceStep * (i % (heightDims * latDims * lonDims) % lonDims)
          DataV.setdata(i, this.ValidTempData_Before(ExtremMap, DataV.dataDouble(i), lat, lon))
        }
      } else {
        //按临界点质控
        val (max: Double, min: Double) = {
          if (fcType.equals(Constant.PRE)) {
            (800.0d, 0.0d)
          } else if (fcType.equals(Constant.WIND)) {
            if (dataName.equals(PropertiesUtil.WIND)) {
              (360.0d, 0.0d)
            } else {
              (80.0d, 0.0d)
            }
          } else if (fcType.equals(Constant.RHU) || fcType.equals(Constant.CLOUD)) {
            (100.0d, 0.0d)
          } else if (fcType.equals(Constant.PRS)) {
            (1080.0d, 300.0d)
          } else {
            (100.0d, 0.0d)
          }
        }
        for (i <- 0 until length) {
          if (DataV.dataDouble(i) > max) {
            if (fcType.equals(Constant.PRE) || fcType.equals(Constant.WIND)) {
              //降水和风速异常大的时候订正值为0
              //              RevisedData(i)=min
              DataV.setdata(i, min)
            } else {
              //              RevisedData(i)=max
              DataV.setdata(i, max)
            }
          } else if (DataV.dataDouble(i) < min) {
            //            RevisedData(i)=min
            DataV.setdata(i, min)
          }
        }
      }

      //      val revisedVar=new Variable(dataName, scidata.apply(dataName).dataType, new Nd4jTensor(RevisedData, shape), scidata.apply(dataName).attributes, dims)
      //      varHash = scidata.variables.map(p => {
      //        p._1 match {
      //          case i if i.equals(dataName) => (p._1, revisedVar)
      //          case _ => p
      //        }
      //      })
      varHash.+=(DataV)
    }

    val sciDatasetName = scidata.datasetName
    val attrs = AttributesOperationCW.addTrace(scidata.attributes, "PostControl")
    val sci = new SciDatasetCW(varHash, attrs, sciDatasetName)

    //--------------------日志采集----------------------------------
    val endTime = System.currentTimeMillis()
    //    val fileList = ""
    //    var basicRemark: JSONObject = new JSONObject()
    //    basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
    //    basicRemark = AttributesOperationCW.addOtherMark(basicRemark, "Fc")
    //    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject()
    //    stateRemark = AttributesOperationCW.addOtherMark(stateRemark, "postControl")
    //    val stateDetailRemark: JSONObject = new JSONObject()
    //    SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
    //--------------------------------------------------------------


    //    val End=System.currentTimeMillis()
    /*    val msg=ws.showDateStr("QualityControl.PostControl",startTime,endTime)
        println(msg)*/

    WeatherShowtime.showDateStrOut1("QualityControl.PostControl", startTime, endTime)

    sci
  }
}
