package com.weather.bigdata.mt.basic.mt_commons.commons.QualityControlUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Offset.DataOffsetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object ForwardControlCW {

  def forwardControl(Rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    this.forwardControl0(Rdd, 0, 1)
  }

  //未完全修复bug隐患,参考72线上流程修复
  private def forwardControl0(Rdd: RDD[SciDatasetCW], replacer_lat: Int, byreplacer_lat: Int): RDD[SciDatasetCW] = {
    Rdd.map(scidata => this.forwardControl0(scidata, replacer_lat, byreplacer_lat))
  }

  def forwardControl(scidata: SciDatasetCW): SciDatasetCW = {
    this.forwardControl0(scidata, 0, 1)
  }

  private def forwardControl0(scidata: SciDatasetCW, replacer_lat: Int, byreplacer_lat: Int): SciDatasetCW = {
    val fcType = AttributesOperationCW.getFcType(scidata)
    val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)

    val dataNames = PropertiesUtil.getrElement_rename(fcType)
    val idName: String = AttributesOperationCW.getIDName(scidata)

    val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]

    val timeV: VariableCW = scidata.apply(timeName)
    val heightV: VariableCW = scidata.apply(heightName)
    val latV: VariableCW = scidata.apply(latName)
    val lonV: VariableCW = scidata.apply(lonName)

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    vals += timeV += heightV += latV += lonV

    dataNames.foreach(dataName => {
      val DataV: VariableCW = scidata.apply(dataName)
      val dataDefault = this.getdataDefault_beforeUnit(fcType)
      val la: Int = 0
      var flag: Boolean = false

      //存在数据bug隐患
      for (i <- 0 until timeLen) {
        val index0 = IndexOperation.getIndex4_thlatlon(i, 0, 0, 0, heightLen, latLen, lonLen)
        val d: Double = math.abs(DataV.dataDouble(index0) - dataDefault)
        if (d < 0.001) {
          for (j <- 0 until heightLen) {
            for (l <- 0 until lonLen) {
              val index = IndexOperation.getIndex4_thlatlon(i, j, la, l, heightLen, latLen, lonLen)
              val index1 = IndexOperation.getIndex4_thlatlon(i, j, la + 1, l, heightLen, latLen, lonLen)
              println("idName=" + idName + ",dataName=" + dataName + ";SingleFrontControl数据订正:Data(" + index + ")=" + DataV.dataDouble(index) + "->" + "Data(" + index1 + ")=" + DataV.dataDouble(index1))
              //              DataV.setdata(index,DataV.dataDouble(index1))
              DataV.setdata(index, DataV.dataDouble(index1))
            }
          }
          flag = true
        }
      }
      if (flag) {
        AttributesOperationCW.addTrace(DataV.attributes, "SingleForwardControl(" + dataName + ")")
        //        vals+=VariableOperationCW.updataVars4(DataV,DataV)
        vals += DataV
      }
    })

    val attr = AttributesOperationCW.addTrace(scidata.attributes, "SingleForwardControl")

    new SciDatasetCW(vals, attr, scidata.datasetName)
    //    AttributesOperation.addTrace(scidata,"SingleForwardControl")
  }

  private def getdataDefault_beforeUnit(fcType: String): Double = 9999.0d

  private def getdataDefault_afterUnit(fcType: String): Double = {
    val default = this.getdataDefault_beforeUnit(fcType)
    if (Constant.TEM.equals(fcType) || Constant.PRS.equals(fcType)) {
      val (radio, offset) = PropertiesUtil.getFcTypeRadioOffset(fcType)
      DataOffsetCW.onlydataoffset(default, radio, offset)
    } else {
      default
    }
  }
}
