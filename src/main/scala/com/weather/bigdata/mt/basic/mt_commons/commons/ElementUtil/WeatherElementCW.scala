package com.weather.bigdata.mt.basic.mt_commons.commons.ElementUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.VariableOperationCW
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadNcCW, WriteNcCW}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object WeatherElementCW {
  val (windUName: String, windVName: String) = (PropertiesUtil.WINU, PropertiesUtil.WINV)
  val (windSGName: String, windDGName: String) = (PropertiesUtil.WINSG, PropertiesUtil.WINDG)
  val (windSName: String, windDName: String) = (PropertiesUtil.WINS, PropertiesUtil.WIND)
  val (windSGDataType: String, windDGDataType: String) = ("Byte", "Byte")
  val (windSDataType: String, windDDataType: String) = ("Double", "Double")
  //  private val VO=VariableOperation
  private val AttO = AttributesOperationCW
  private val ws: WeatherShowtime = new WeatherShowtime

  def WindUV2SD(rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.WindUV2SD(scidata))
  }

  def WindUV2SD(scidata: SciDatasetCW): SciDatasetCW = {
    //    val startTime = System.currentTimeMillis()
    val (timeName, heightName, latName, lonName) = AttO.getVarName(scidata)
    val fcType = AttO.getFcType(scidata)

    val idName = AttO.getIDName(scidata)
    val timeV = scidata.apply(timeName)
    val heightV = scidata.apply(heightName)
    val latV = scidata.apply(latName)
    val lonV = scidata.apply(lonName)
    val winduV: VariableCW = scidata.apply(windUName)
    val windvV: VariableCW = scidata.apply(windVName)

    val (windsV: VariableCW, winddV: VariableCW) = this.UV2SD(timeV, heightV, latV, lonV, winduV, windvV)

    val vars = new ArrayBuffer[VariableCW]
    vars += timeV += heightV += latV += lonV += windsV += winddV

    val attr = scidata.attributes
    val datasetName = scidata.datasetName
    //    val varHash=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))
    val attrnew = AttO.addTrace(attr, "WindUV2SD")

    val scidata0 = new SciDatasetCW(vars, attrnew, datasetName)

    //--------------------日志采集----------------------------------
    //    val endTime = System.currentTimeMillis()
    //    val fileList = ""
    //    var basicRemark: JSONObject = new JSONObject()
    //    basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
    //    val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject()
    //    stateRemark = AttributesOperationCW.addOtherMark(stateRemark, "WindUV2SD")
    //    val stateDetailRemark: JSONObject = new JSONObject()
    //    SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
    //--------------------------------------------------------------

    scidata0
  }

  private def UV2SD(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): (VariableCW, VariableCW) = {
    val S = this.UV2S(timeV, heightV, latV, lonV, winduV, windvV)
    val D = this.UV2D(timeV, heightV, latV, lonV, winduV, windvV)
    (S, D)
  }

  private def UV2S(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): VariableCW = {
    //原始的分隔后的数组
    //    val windUdata = winduV
    //    val windVdata = windvV.data


    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()
    //原始数组长度

    val timeheightlatlonLen = timeLen * heightLen * latLen * lonLen

    val dims = List((timeV.name, timeLen), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))
    val shape = Array(timeLen, heightLen, latLen, lonLen)
    val Sdata = new Array[Double](timeheightlatlonLen)
    for (i <- 0 until timeheightlatlonLen - 1) {
      Sdata(i) = WindRule.WindUV2S(winduV.dataDouble(i), windvV.dataDouble(i))
    }
    val attr = AttO.creatAttribute()
    VariableOperationCW.creatVars4(Sdata, this.windSName, timeV.name, heightV.name, latV.name, lonV.name, timeLen, heightLen, latLen, lonLen)
    //    new Variable(this.windSName, windSDataType, new Nd4jTensor(Sdata, shape), attr, dims)
  }

  def WindUV2SDG(rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.WindUV2SDG(scidata))
  }

  def WindUV2SDG(scidata: SciDatasetCW): SciDatasetCW = {
    val (timeName, heightName, latName, lonName) = AttO.getVarName(scidata)
    val fcType = AttO.getFcType(scidata)

    val idName = AttO.getIDName(scidata)
    val timeV = scidata.apply(timeName)
    val heightV = scidata.apply(heightName)
    val latV = scidata.apply(latName)
    val lonV = scidata.apply(lonName)
    val winduV: VariableCW = scidata.apply(windUName)
    val windvV: VariableCW = scidata.apply(windVName)

    val (windsgV: VariableCW, winddgV: VariableCW) = this.UV2SDG(timeV, heightV, latV, lonV, winduV, windvV)

    /*
        val vars=new ArrayBuffer[Variable]
        vars+=timeV+=heightV+=latV+=lonV+=windsgV+=winddgV

        val attr=scidata.attributes
        val datasetName= scidata.datasetName
        val varHash=VariableOperation.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))
        val attrnew=AttO.addTrace(attr,"WindUV2SDG")

        new SciDataset(varHash,attrnew,datasetName)*/

    scidata.variables.remove(windUName)
    scidata.variables.remove(windVName)
    scidata.update(windsgV.name, windsgV)
    scidata.update(winddgV.name, winddgV)
    AttributesOperationCW.addTrace(scidata, "WindUV2SDG")

    scidata
  }

  private def UV2SDG(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): (VariableCW, VariableCW) = {
    val (windsV, winddV) = this.UV2SD(timeV, heightV, latV, lonV, winduV, windvV)
    val SG = this.S2SG(timeV, heightV, latV, lonV, windsV)
    val DG = this.D2DG(timeV, heightV, latV, lonV, winddV)
    (SG, DG)
  }

  def WindSD2SDG(rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.WindSD2SDG(scidata))
  }

  def main(args: Array[String]): Unit = {
    val infile = "/D:/Data/forecast/Output/NMC/J50_WINSD_12.0.nc"
    val outfile = "/D:/Data/forecast/Output/NMC/test1/"
    var rdd = ReadNcCW.ReadNcScidata(infile)
    rdd = this.WindSD2SDG(rdd)
    WriteNcCW.WriteNcFile_test(rdd, outfile)
  }

  def WindSD2SDG(scidata: SciDatasetCW): SciDatasetCW = {
    val Start = System.currentTimeMillis()
    val (timeName, heightName, latName, lonName) = AttO.getVarName(scidata)
    val fcType = AttO.getFcType(scidata)

    val timeV = scidata.apply(timeName)
    val heightV = scidata.apply(heightName)
    val latV = scidata.apply(latName)
    val lonV = scidata.apply(lonName)

    val vars = new ArrayBuffer[VariableCW]
    vars += timeV += heightV += latV += lonV += this.S2SG(timeV, heightV, latV, lonV, scidata.apply(windSName)) += this.D2DG(timeV, heightV, latV, lonV, scidata.apply(windDName))

    val idname = AttO.getIDName(scidata)
    val attr = scidata.attributes
    val attrnew = AttO.addTrace(attr, "WindSD2SDG")
    val datasetName = scidata.datasetName

    //    val varHash=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idname,vars,(timeName,heightName,latName,lonName))

    val End = System.currentTimeMillis()
    ws.showDateStrOut1("WindSD2SDG:", Start, End)

    new SciDatasetCW(vars, attrnew, datasetName)
  }

  private def UV2SG(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): VariableCW = {
    val windsV = this.UV2S(timeV, heightV, latV, lonV, winduV, windvV)
    val windSGV = this.S2SG(timeV, heightV, latV, lonV, windsV)
    windSGV
  }

  private def S2SG(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, windsV: VariableCW): VariableCW = {
    //原始的分隔后的数组
    //    val windSdata = windsV.data

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    // 原始数组长度
    val timeheightlatlonLen = timeLen * heightLen * latLen * lonLen

    val dims = List((timeV.name, timeLen), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))
    val shape = Array(timeLen, heightLen, latLen, lonLen)
    val SGdata = new Array[Byte](timeheightlatlonLen)
    for (i <- 0 until timeheightlatlonLen - 1) {
      val speed = windsV.dataDouble(i)
      SGdata(i) = WindRule.WindS2SG(speed)
    }
    val attr = AttO.creatAttribute()
    //    new Variable(this.windSGName, windSGDataType, new Nd4jTensor(SGdata, shape), attr, dims)
    VariableOperationCW.creatVars4(SGdata, this.windSGName, timeV.name, heightV.name, latV.name, lonV.name, timeLen, heightLen, latLen, lonLen)
  }

  private def UV2DG(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): VariableCW = {
    val winddV = this.UV2D(timeV, heightV, latV, lonV, winduV, windvV)
    val windDGV = this.D2DG(timeV, heightV, latV, lonV, winddV)
    windDGV
  }

  private def UV2D(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winduV: VariableCW, windvV: VariableCW): VariableCW = {
    //原始的分隔后的数组
    //    val windUdata = winduV.data
    //    val windVdata = windvV.data

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()
    //原始数组长度

    val timeheightlatlonLen = timeLen * heightLen * latLen * lonLen

    val dims = List((timeV.name, timeLen), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))
    val shape = Array(timeLen, heightLen, latLen, lonLen)
    val Ddata = new Array[Double](timeheightlatlonLen)
    for (i <- 0 until timeheightlatlonLen - 1) {
      Ddata(i) = WindRule.WindUV2D(winduV.dataDouble(i), windvV.dataDouble(i))
    }
    val attr = AttO.creatAttribute()
    //    new Variable(this.windDName, windDDataType, new Nd4jTensor(Ddata, shape), attr, dims)
    VariableOperationCW.creatVars4(Ddata, this.windDName, timeV.name, heightV.name, latV.name, lonV.name, timeLen, heightLen, latLen, lonLen)
  }

  private def D2DG(timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, winddV: VariableCW): VariableCW = {
    //原始的分隔后的数组
    //    val windDdata = winddV.data

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    //原始数组长度
    val timeheightlatlonLen = timeLen * heightLen * latLen * lonLen

    val dims = List((timeV.name, timeLen), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))
    val shape = Array(timeLen, heightLen, latLen, lonLen)
    val DGdata = new Array[Byte](timeheightlatlonLen)
    for (i <- 0 until timeheightlatlonLen - 1) {
      DGdata(i) = WindRule.WindD2DG(winddV.dataDouble(i))
    }
    val attr = AttO.creatAttribute()
    //    new Variable(this.windDGName, windDGDataType, new Nd4jTensor(DGdata, shape), attr, dims)
    VariableOperationCW.creatVars4(DGdata, this.windDGName, timeV.name, heightV.name, latV.name, lonV.name, timeLen, heightLen, latLen, lonLen)
  }
}
