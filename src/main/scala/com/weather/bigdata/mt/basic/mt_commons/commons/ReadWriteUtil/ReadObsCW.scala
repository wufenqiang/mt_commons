package com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil

import java.util.Date

import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ReadObsCW {
  def ReadObs (fileName: String, fcTypes: Array[String]): SciDatasetCW = {
    val latName_Obs = PropertiesUtil.latName_Obs
    val lonName_Obs = PropertiesUtil.lonName_Obs

    val dataNames_Obs = PropertiesUtil.getObsDataName(fcTypes)
    val vars: ArrayBuffer[String] = new ArrayBuffer[String]( )
    vars += latName_Obs += lonName_Obs
    dataNames_Obs.foreach(dataName_Obs => (vars += dataName_Obs))

    val obsData = ReadNcCW.ReadNcScidata(fileName, vars)
    obsData
  }

  //key:(stnId,obsdate,fcType)
  private def readObsMap1 (obsfile: String, obsdate: Date, fileHeadLine: Int, splitStr: String, stnIdCol: Int, fcTypesCol: Array[(String, Int)], oldMap: mutable.HashMap[(String, Date, String), Double] = new mutable.HashMap[(String, Date, String), Double]): mutable.HashMap[(String, Date, String), Double] = {
    val obsmsgLine: Int = 2
    var obsArr: Array[String] = HDFSReadWriteUtil.readTXT(obsfile)
    var eleNum: Int = 8
    for (i <- 0 to fileHeadLine - 1) {
      if (i == (obsmsgLine - 1)) {
        eleNum = obsArr.head.split(splitStr)(1).toInt
      }
      obsArr = ArrayOperation.minusHead(obsArr)
    }
    //    println("obsFile,eleNum="+eleNum)
    obsArr.filter(p => (!p.equals(""))).filter(p => {
      val lineArr: Array[String] = p.split(splitStr)
      lineArr.length == (eleNum + 1)
    }).foreach(obsStr => {
      val lineArr: Array[String] = obsStr.split(splitStr)
      val stnId = lineArr(stnIdCol)
      fcTypesCol.foreach(fcTypeLine => {
        val fcType = fcTypeLine._1
        val fcTypecol = fcTypeLine._2
        val fcTypeValue: Double = lineArr(fcTypecol).toDouble
        oldMap.put((stnId, obsdate, fcType), fcTypeValue)
      })
    })
    println("obsMap:" + obsfile + " 最终输出大小：" + oldMap.size)
    oldMap
  }

  //key:(ocfdate,stationId,fcdate,fcType)
  private def readOcfMap1 (ocffile: String, ocfdate: Date, timeStep: Double, fileHeadLine: Int, splitStr: String, fcTypesCol: Array[(String, Int)], oldMap: mutable.HashMap[(Date, String, Date, String), Double] = new mutable.HashMap[(Date, String, Date, String), Double]): mutable.HashMap[(Date, String, Date, String), Double] = {
    var ocfArr: Array[String] = {
      HDFSReadWriteUtil.readTXT(ocffile)
    }

    if (ocfArr.length > fileHeadLine) {
      var stationId: String = null
      for (i <- 0 to fileHeadLine - 1) {
        ocfArr = ArrayOperation.minusHead(ocfArr)
      }
      ocfArr.foreach(lineTxt => {
        val array = lineTxt.trim.split("\\s++")
        val len = array.length

        if (len < 11) { //站号行
          stationId = array(0)
        } else {
          val time: String = {
            if (timeStep == 1.0d || timeStep == 3.0d) {
              //有11个要素
              if (!array(0).equals("")) {
                array(2)
                //            eleArray ++= array.slice(3, 14) //slice(from: Int, until: Int): List[A] 提取列表中从位置from到位置until(不含该位置)的元素列表
              } else {
                array(3)
                //            eleArray ++= array.slice(4, 15)
              }
            } else if (timeStep == 12.0d) {
              //有13个要素
              if (!array(0).equals("")) {
                array(1)
                //            eleArray ++= array.slice(2, 15)
              }
              else {
                array(2)
                //            eleArray ++= array.slice(3, 16)
              }
            } else {
              ""
            }
          }

          //在素有要素数组中提取需要的温度、降水和天气现象要素,通过列数确定要素对应ocf文件中的位置

          val fcdate: Date = DateFormatUtil.YYYYMMDDHH0(time)

          if (timeStep == 1.0d) {
            fcTypesCol.foreach(f => {
              val fcType = f._1
              val fcTypeCol = f._2
              val key: (Date, String, Date, String) = (ocfdate, stationId, fcdate, fcType)
              val value = array(fcTypeCol).toDouble
              oldMap.put(key, value)
            })

          } else if (timeStep == 12.0d) {
            fcTypesCol.foreach(f => {
              val fcType = f._1
              val fcTypeCol = f._2
              val key: (Date, String, Date, String) = (ocfdate, stationId, fcdate, fcType)
              val value = array(fcTypeCol).toDouble
              oldMap.put(key, value)
            })
          } else {
            println("readOcfMap,timeStep=" + timeStep + ",没有配置")
          }
        }
      })
    }
    println("ocfMap:" + ocffile + " 最终输出大小：" + oldMap.size)
    oldMap
  }
}
