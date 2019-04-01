package com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Interception

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesConstant, AttributesOperationCW}
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{IndexOperation, VariableOperationCW, ucarma2ArrayOperationCW}
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.ArrayOperation
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType

import scala.collection.mutable.ArrayBuffer

object DataInterceptionCW {
  private val VO = VariableOperationCW
  private def datacutByTime0(scidata: SciDatasetCW, dataNames: Array[String], times: Array[Double]): SciDatasetCW = {
    if (times == null) {
      null
    } else {
      val idname = AttributesOperationCW.getIDName(scidata)
      val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)
      val fcType = AttributesOperationCW.getFcType(scidata)

      val timeV: VariableCW = scidata.apply(timeName)
      val time: Array[Double] = timeV.dataDouble()
      if (time.toList == times.toList) {
        scidata
      } else {

        val timesV: VariableCW = VO.creatVar(times, timeV)
        val heightV: VariableCW = scidata.apply(heightName)
        val latV: VariableCW = scidata.apply(latName)
        val lonV: VariableCW = scidata.apply(lonName)
        val datasV: Array[VariableCW] = dataNames.map(dataName => scidata.apply(dataName))


        //        val height: Array[Double] = heightV.data
        //        val lat: Array[Double] = latV.data
        //        val lon: Array[Double] = lonV.data

        val heightLen = heightV.getLen()
        val latLen = latV.getLen()
        val lonLen = lonV.getLen()

        var timeIndexs: Array[Int] = ArrayOperation.HashMapSearch(time, times)

        val datas0: Array[VariableCW] = datasV.map(dataV => {
          //          val data = dataV.data
          val data0: Array[Double] = new Array[Double](timeIndexs.length * heightLen * latLen * lonLen)
          var m: Int = 0
          for (i <- timeIndexs) {
            for (j <- 0 to heightLen - 1) {
              for (k <- 0 to latLen - 1) {
                for (l <- 0 to lonLen - 1) {
                  val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                  data0(m) = dataV.dataDouble(index)
                  m += 1
                }
              }
            }
          }

          val Vattr = dataV.attributes
          val newdataName = dataV.name
          val vDataType = dataV.dataType

          val shape = Array(timeIndexs.length, heightLen, latLen, lonLen)

          val dims = List((timeV.name, times.length), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))

          VariableOperationCW.creatVars4(data0, dataV)
          //          new Variable(newdataName, vDataType, new Nd4jTensor(data0, shape), Vattr, dims)
        })

        val timeMin = times.min
        val timeMax = times.max
        val timeStep = {
          if (times.length > 1) {
            times(1) - times(0)
          } else {
            0
          }
        }

        val varHash = new ArrayBuffer[VariableCW]
        varHash += timesV += heightV += latV += lonV
        datas0.foreach(
          data0 => {
            varHash += data0
          }
        )
        val datasetName = scidata.datasetName
        val attrs = scidata.attributes
        val idName = AttributesOperationCW.getIDName(scidata)
        val attrsnew = AttributesOperationCW.addTrace(attrs, "DataInterception(" + timeMin + ":" + timeStep + ":" + timeMax + ")")
        //        val varHash = VO.allVars2NameVar4(this.getClass.toString, idName, dataArr, (timeName, heightName, latName, lonName))
        new SciDatasetCW(varHash, attrsnew, datasetName)
      }
    }
  }
  private def datacutByTime1(scidata: SciDatasetCW, dataNames: Array[String], times: Array[Double]): SciDatasetCW = {
    if (times == null) {
      null
    } else {
      val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(scidata)
      val timeV: VariableCW = scidata.apply(timeName)
      val time: Array[Double] = timeV.dataDouble()
      if (time.sameElements(times)) {
        scidata
      } else {

        val heightV: VariableCW=scidata.apply(heightName)
        val latV: VariableCW=scidata.apply(latName)
        val lonV:VariableCW=scidata.apply(lonName)

        val timesLen = times.length
        val heightLen = heightV.getLen()
        val latLen = latV.getLen()
        val lonLen = lonV.getLen()

        val timeIndexs: Array[Int] = ArrayOperation.HashMapSearch(time, times)
        dataNames.foreach(dataName => {
          val data0V = scidata.apply(dataName)
          val shape=Array(timesLen,heightLen ,latLen ,lonLen)
          val data=ucar.ma2.Array.factory(DataType.getType(data0V.dataType),shape)
          var m: Int = 0
          timeIndexs.foreach(timeIndex => {
            for (j <- 0 to heightLen - 1) {
              for (k <- 0 to latLen - 1) {
                for (l <- 0 to lonLen - 1) {
                  val index = IndexOperation.getIndex4_thlatlon(timeIndex, j, k, l, heightLen, latLen, lonLen)
                  ucarma2ArrayOperationCW.setObject(data,m,data0V.dataFloat(index))
                  m += 1
                }
              }
            }
          })

          val dataV=VariableOperationCW.creatVars4(data, data0V)
          scidata.update(dataName, dataV)
        })

        val idName = AttributesOperationCW.getIDName(scidata)
        println(new Date() + ",idName=" + idName + ",datacutByTime1")
        scidata.update(timeName, VariableOperationCW.creatVar(times, timeV))

        AttributesOperationCW.addTrace(scidata, "DataInterception")
      }
    }


  }
  def datacutByTime(rdd: RDD[SciDatasetCW], dataNames: Array[String], times: Array[Double]): RDD[SciDatasetCW] = {
    if (rdd == null) {
      null
    } else {
      if (times == null) {
        rdd
      } else {
        rdd.map(scidata => this.datacutByTime(scidata, dataNames, times))
      }
    }
  }
  def datacutByTime(scidata: SciDatasetCW, dataNames: Array[String], times: Array[Double]): SciDatasetCW = {
    this.datacutByTime1(scidata, dataNames, times)
  }

  //by Lijing on 2018/5/15
  /*def dataunionByTime(fcdate: Date, cutTime: Int, arrRddKV: RDD[(String, Iterable[SciDatasetCW])], times: Array[Double], dataNames: Array[String]): RDD[SciDatasetCW] = {

    val unionrdd: RDD[SciDatasetCW] = arrRddKV.map(
      RddKV => {
        //        println(RddKV._1)

        val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(RddKV._2.last)
        val fcdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
        val scidata = RddKV._2.find(sci => (sci.attributes.get(AttributesConstant.dateKey).get.equals(fcdateStr))).get
        val scidata2 = RddKV._2.find(sci => (!sci.attributes.get(AttributesConstant.dateKey).get.equals(fcdateStr))).get
        val timeV: VariableCW = scidata.apply(timeName)
        val timesV: VariableCW = VariableOperationCW.creatVar(times, timeV)
        val heightV: VariableCW = scidata.apply(heightName)
        val latV: VariableCW = scidata.apply(latName)
        val lonV: VariableCW = scidata.apply(lonName)
        val datasV: Array[VariableCW] = dataNames.map(dataName => scidata.apply(dataName))
        val datasV2: Array[VariableCW] = dataNames.map(dataName => scidata2.apply(dataName))

        val time: Array[Double] = timeV.dataDouble()
        val height: Array[Double] = heightV.dataDouble()
        val lat: Array[Double] = latV.dataDouble()
        val lon: Array[Double] = lonV.dataDouble()
        val timeLen = time.length
        val heightLen = height.length
        val latLen = lat.length
        val lonLen = lon.length

        val datas0: Array[VariableCW] = new Array[VariableCW](datasV.length)

        for (i <- 0 until datasV.length) {
          val data = datasV.apply(i).dataDouble()
          val data2 = datasV2.apply(i).dataDouble()
          val data0: Array[Double] = new Array[Double](time.length * height.length * lat.length * lon.length)
          var m: Int = 0
          var m2: Int = 0
          for (i <- 0 to timeLen - 1) {
            for (j <- 0 to heightLen - 1) {
              for (k <- 0 to latLen - 1) {
                for (l <- 0 to lonLen - 1) {
                  val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                  if (time(i) > cutTime) {
                    data0(m) = data(index - m2)
                  } else {
                    data0(m) = data2(index)
                    m2 += 1
                  }
                  m += 1
                }
              }
            }
          }
          val dataV = datasV.apply(i)
          val Vattr = dataV.attributes
          val newdataName = dataV.name
          val vDataType = dataV.dataType
          val shape = Array(timeLen, heightLen, latLen, lonLen)
          val dims = List((timeV.name, times.length), (heightV.name, height.length), (latV.name, lat.length), (lonV.name, lon.length))
          //          datas0(i)=new Variable(newdataName, vDataType, new Nd4jTensor(data0, shape),Vattr, dims)
          datas0(i) = VariableOperationCW.creatVars4(data0, dataV)
        }

        val dataArr = new ArrayBuffer[VariableCW]()
        dataArr += timesV += heightV += latV += lonV
        datas0.foreach(
          data0 => {
            dataArr += data0
          }
        )
        val datasetName = scidata.datasetName
        val attrs = scidata.attributes

        //        val varHash=VO.allVars2NameVar4(this.getClass.toString,datasetName,dataArr,(timeName,heightName,latName,lonName))
        new SciDatasetCW(dataArr, attrs, datasetName)
      }
    )
    unionrdd

  }*/
  //by WuFenQiang on 2018/10/14
  def dataunionByTime(fcdate: Date, cutTime: Int, IterableKV: Iterable[SciDatasetCW], times: Array[Double], dataNames: Array[String]): SciDatasetCW = {

    val (timeName, heightName, latName, lonName) = AttributesOperationCW.getVarName(IterableKV.last)
    val fcdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)
    val scidata0 = IterableKV.find(sci => (sci.attributes.get(AttributesConstant.dateKey).get.equals(fcdateStr))).get
    val scidata1 = IterableKV.find(sci => (!sci.attributes.get(AttributesConstant.dateKey).get.equals(fcdateStr))).get
    val timeV: VariableCW = scidata0.apply(timeName)
    val timesV: VariableCW = VariableOperationCW.creatVar(times, timeV)
    val heightV: VariableCW = scidata0.apply(heightName)
    val latV: VariableCW = scidata0.apply(latName)
    val lonV: VariableCW = scidata0.apply(lonName)


    val vals = new ArrayBuffer[VariableCW]()
    vals += timesV += heightV += latV += lonV

    val timesLen = timesV.getLen
    val heightLen = heightV.getLen
    val latLen = latV.getLen
    val lonLen = lonV.getLen

//    val datasV0: Array[VariableCW] = dataNames.map(dataName => scidata0.apply(dataName))
//    val datasV1: Array[VariableCW] = dataNames.map(dataName => scidata1.apply(dataName))
//    val datas0: Array[VariableCW] = new Array[VariableCW](datasV0.length)
//
//    for (i <- 0 until datasV0.length) {
//      val data = datasV0.apply(i).dataDouble()
//      val data2 = datasV1.apply(i).dataDouble()
//      val data0: Array[Double] = new Array[Double](timesLen * heightLen * latLen * lonLen)
//      var m: Int = 0
//      var m2: Int = 0
//      for (i <- 0 to timesLen - 1) {
//        for (j <- 0 to heightLen - 1) {
//          for (k <- 0 to latLen - 1) {
//            for (l <- 0 to lonLen - 1) {
//              val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
//              if (timeV.dataFloat(i) > cutTime) {
//                data0(m) = data(index - m2)
//              } else {
//                data0(m) = data2(index)
//                m2 += 1
//              }
//              m += 1
//            }
//          }
//        }
//      }
//      val dataV = datasV.apply(i)
//      val Vattr = dataV.attributes
//      val newdataName = dataV.name
//      val vDataType = dataV.dataType
//      val shape = Array(timeLen, heightLen, latLen, lonLen)
//      val dims = List((timeV.name, timeLen), (heightV.name, heightLen), (latV.name, latLen), (lonV.name, lonLen))
//      //      datas0(i)=new Variable(newdataName, vDataType, new Nd4jTensor(data0, shape),Vattr, dims)
//      datas0(i) = VariableOperationCW.creatVars4(data0, dataV)
//    }

    dataNames.foreach(dataName=>{
      val dataV0=scidata0.apply(dataName)
      val dataV1=scidata1.apply(dataName)
      val shape=Array(timesLen, heightLen, latLen, lonLen)
      val data=ucar.ma2.Array.factory(DataType.getType(dataV0.dataType),shape)
      var m: Int = 0
      var m2: Int = 0
      for (i <- 0 to timesLen - 1) {
        if(timeV.dataFloat(i) > cutTime){
          for (j <- 0 to heightLen - 1) {
            for (k <- 0 to latLen - 1) {
              for (l <- 0 to lonLen - 1) {
                val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                ucarma2ArrayOperationCW.setObject(data,m,dataV0.dataFloat(index-m2))
                m += 1
              }
            }
          }
        }else{
          for (j <- 0 to heightLen - 1) {
            for (k <- 0 to latLen - 1) {
              for (l <- 0 to lonLen - 1) {
                val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
                ucarma2ArrayOperationCW.setObject(data,m,dataV1.dataFloat(index))
                m2 += 1
                m += 1
              }
            }
          }
        }
      }

      vals += VariableOperationCW.creatVars4(data,dataV0)
    })
    val datasetName = scidata0.datasetName

    var attrs = scidata0.attributes
    attrs=AttributesOperationCW.addTrace(attrs,"dataunionByTime")
    new SciDatasetCW(vals, attrs, datasetName)
  }

  def main(args: Array[String]): Unit = {
    val arr0 = Array(1.0d, 12.0d, 13.0d)
    val arr1 = Array(12.0d, 12.0d, 1.0d, 13.0d)
    //    println(arr0.sameElements(arr1))

  }
}
