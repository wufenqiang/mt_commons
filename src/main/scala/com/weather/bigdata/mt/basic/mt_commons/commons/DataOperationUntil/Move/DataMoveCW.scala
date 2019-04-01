package com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Move

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.DateOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object DataMoveCW {
  private val dateOp = new DateOperation

  def datamove(rdd: RDD[SciDatasetCW], timeStep: Double, hours: Double): RDD[SciDatasetCW] = {
    rdd.map(scidata => this.datamove(scidata, timeStep, hours))
  }

  def datamove(scidata: SciDatasetCW, timeStep: Double, hours: Double): SciDatasetCW = {
    if (scidata == null) {
      scidata
    } else {
      val fcType = AttributesOperationCW.getFcType(scidata)
      val idName = AttributesOperationCW.getIDName(scidata)
      val fcdateold = AttributesOperationCW.getfcdate(scidata)
      val fcdatenew = dateOp.plusHour(fcdateold, hours.toInt)

      val fcdateoldStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdateold)
      val fcdatenewStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdatenew)

      val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)

      val dataNames = PropertiesUtil.getwElement(fcType, timeStep)

      val timeV: VariableCW = scidata.apply(timeName)
      val heightV: VariableCW = scidata.apply(heightName)
      val latV: VariableCW = scidata.apply(latName)
      val lonV: VariableCW = scidata.apply(lonName)

      //      val time=timeV.dataDouble()
      //      val height=heightV.dataDouble
      //      val lat=latV.dataDouble
      //      val lon=lonV.dataDouble

      val timeLen = timeV.getLen()
      val heightLen = heightV.getLen()
      val latLen = latV.getLen()
      val lonLen = lonV.getLen()

      /*val datasV:Array[Variable]=dataNames.map(dataName=>scidata.apply(dataName))*/

      val vals: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]
      vals += timeV += heightV += latV += lonV


      val N: Int = (hours / timeStep).toInt

      dataNames.foreach(
        dataName => {
          val dataV = scidata.apply(dataName)
          //          val data=dataV.data
          for (i <- 0 to timeLen - 1) {
            if ((i + N) < timeLen) {
              for (j <- 0 to heightLen - 1) {
                for (k <- 0 to latLen - 1) {
                  for (l <- 0 to lonLen - 1) {
                    val index0 = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                    val index1 = IndexOperation.getIndex4_thlatlon(i + N, j, k, l, heightLen, latLen, lonLen)
                    dataV.setdata(index0, dataV.dataDouble(index1))
                  }
                }
              }
            }
          }
          //          val dataV0=VariableOperation.updataVars4(data,dataV)
          vals += dataV
        }
      )

      //      val vash: ArrayBuffer[(String,Variable)]=VariableOperation.allVars2NameVar4(this.getClass.getName,idName,vals,(timeName,heightName,latName,lonName))
      AttributesOperationCW.updatefcdate(scidata, fcdatenew)
      //      var att0=scidata.attributes
      AttributesOperationCW.addTrace(scidata, "ChangeReftime[" + fcdateoldStr + "=>" + fcdatenewStr + "]")
      //      new SciDatasetCW(vals,att0,scidata.datasetName)
      scidata
    }
  }

  def main(args:Array[String]): Unit ={

  }
}
