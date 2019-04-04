package com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil

import java.text.DecimalFormat
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil
import com.weather.bigdata.it.utils.operation.{JsonOperation, MapOperation, NumOperation}
import com.weather.bigdata.it.utils.{ReadWrite, ShowUtil, WeatherShowtime}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ReadFile {
  //Map[latlon,StnId]
  private val latlonSplit: String = ","
  private val checkStationOpen: Boolean = PropertiesUtil.checkStationOpen

  def ReadJsonRdd(splitFile: String): RDD[JSONObject] = {
    val sc = ContextUtil.getSparkContext( )

    /*
    val JsonStrings:RDD[String] = sc.textFile(fileName)./*groupBy(f=>f).*/coalesce(splitNum,true)/*.map(f=>f._2.last)*/
    val rdd_JsonObject:RDD[JSONObject]=JsonStrings.map(jsonString=>jsonOp.Str2JSONObject(jsonString))
    rdd_JsonObject*/

    val jsonArr: Array[JSONObject] = this.ReadJsonArr(splitFile)
    val splitNum = jsonArr.length
    //    val jsonRdd:RDD[JSONObject]=sc.makeRDD(jsonArr,splitNum)
    val jsonRdd: RDD[JSONObject] = sc.parallelize(jsonArr, splitNum).coalesce(splitNum, true)
    jsonRdd
  }

  def ReadJsonArr(splitFile: String): Array[JSONObject] = {
    println("jsonFile=" + splitFile)
    HDFSReadWriteUtil.readTXT(splitFile).map(s => JsonOperation.Str2JSONObject(s))
  }

  def ReadOcfFile0(ocfFileName: String, timeStep: Double, ocfdate: Date, appId: String): Map[String, Map[String, Map[String, Float]]] = {
    val ocf: Map[String, Map[String, Map[String, Float]]] = this.readOcf0(ocfFileName, timeStep)

    //-----------------Ocf数据采集日志接入-------------------------------
    //    val changePerson: String = "罗冰"
    //    //Ocf数据上游采集人员
    //    var basicRemark: JSONObject = new JSONObject
    //    basicRemark = AttributesOperation.addAsyOpen(basicRemark)
    //    basicRemark = AttributesOperation.addSourceDataType(basicRemark, Constant.OcfKey)
    //    basicRemark = AttributesOperation.addTimeStep(basicRemark, timeStep)
    //    basicRemark = AttributesOperation.addocffile(basicRemark, ocfFileName)
    //    basicRemark = AttributesOperation.addAppId(basicRemark, appId)
    //    val collectionflagStr: String = {
    //      if (ocf.isEmpty) {
    //        "N"
    //      } else {
    //        "Y"
    //      }
    //    }
    //    //    val sizeFlagStr:String="N"
    //    val sizeFlagStr: String = SendMsg.ocfsizeFlagStr(timeStep, ocfFileName)
    //    val arrFlagStr: String = SendMsg.ocfArrFlagStr(ocfdate, ocfFileName: String)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject
    //    stateRemark = AttributesOperation.addOtherMark(stateRemark, "Ocf数据源不做质控,暂不对文件大小做质控")
    //    SendMsg.collectionSend(ocfFileName, changePerson, basicRemark, collectionflagStr, sizeFlagStr, arrFlagStr, qualityFlagStr, stateRemark)
    //----------------------------------------------------------------------

    ocf
  }

  //  def readOcf (ocfFileName: String, timeStep: Double): Map[String, Map[String, Map[String, Float]]] = {
  //    this.ReadOcf0(ocfFileName, timeStep)
  //  }
  def readOcf0(ocfFileName: String, timeStep: Double): Map[String, Map[String, Map[String, Float]]] = {
    this.ReadOcf0(ocfFileName, timeStep)
  }

  def ReadTemExFile (TemExFile: String): Map[String, Map[String, Array[Float]]] = {
    val arr: Array[String] = HDFSReadWriteUtil.readTXT(TemExFile)

    val length = 12
    var TemExMap: Map[String, Map[String, Array[Float]]] = Map( )
    var ExTypeMap: Map[String, Array[Float]] = Map( )
    var lineNum = 0
    arr.foreach(lineTxt => {
      lineNum += 1
      var array = lineTxt.split(",")
      var areaName = array(0)
      var exType = array(1)
      var exTemp = new Array[Float](length)
      for (i <- 0 until length) {
        exTemp(i) = array(i + 2).toFloat
      }
      if (lineNum % 2 == 1) {
        ExTypeMap = Map( )
      }
      ExTypeMap += (exType -> exTemp)
      if (TemExMap.contains(areaName)) {
        var tmpMap = TemExMap.get(areaName)
        TemExMap += (areaName -> ExTypeMap)
      } else {
        TemExMap += (areaName -> ExTypeMap)
      }

    })

    println("ReadTemExFile=" + TemExFile + ",TemExMap 最终输出大小：" + TemExMap.size)
    TemExMap


  }

  def ReadStationInfoMap_IdGetlatlon (StnFile: String, FileHeaderLines: Int, stnIdCols: Int, latCols: Int, lonCols: Int, splitStr: String): mutable.HashMap[String, String] = {
    if (this.checkStationOpen) {
      val latlonGetId = this.ReadStationInfoMap_latlonGetId(StnFile: String, FileHeaderLines: Int, stnIdCols: Int, latCols: Int, lonCols: Int, splitStr: String)
      MapOperation.MapTranspose(latlonGetId)
    } else {
      null
    }
  }

  def ReadStationInfoMap_latlonGetId (StnFile: String, FileHeaderLines: Int, stnIdCols: Int, latCols: Int, lonCols: Int, splitStr: String): mutable.HashMap[String, String] = {
    if (this.checkStationOpen) {
      val decimalFormat: DecimalFormat = new DecimalFormat(".000")
      val stationMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]( )
      val stationsInfo: Array[String] = ReadFile.ReadStationInfo(StnFile)
      for (i <- FileHeaderLines to stationsInfo.length - 1) {
        val stationInfo = stationsInfo(i)
        val stationArr = stationInfo.split(splitStr)
        if (stationArr.length == 1) {
          val msg = "站表分隔符出错/i=" + i
          val e: Exception = new Exception(msg)
          e.printStackTrace( )
        } else {
          val stnId: String = stationArr(stnIdCols)
          val lat: String = decimalFormat.format(NumOperation.K3dec(stationArr(latCols).toDouble))
          val lon: String = decimalFormat.format(NumOperation.K3dec(stationArr(lonCols).toDouble))
          val LatLon = lat + this.latlonSplit + lon
          stationMap.put(LatLon, stnId)
        }
      }

      if (stationMap.isEmpty) {
        val msg = "站表Map为空,请检查ReadStationInfoMap_latlonGetId接口"
        val e: Exception = new Exception(msg)
        e.printStackTrace( )
        //-----------------------------日志采集-------------------------------------

        //--------------------------------------------------------------------------
      }
      stationMap
    } else {
      null
    }
  }

  def ReadStationInfoMap_latlonGetId0 (): mutable.HashMap[String, String] = {
    val stationfile = PropertiesUtil.stationfile0
    val FileHeaderLines = 0
    val stnIdCols = 0
    val latCols = 2
    val lonCols = 1
    val splitStr = ","
    this.ReadStationInfoMap_latlonGetId(stationfile, FileHeaderLines, stnIdCols, latCols, lonCols, splitStr)
  }

  def ReadStationInfoMap_IdGetlatlon1 (): mutable.HashMap[String, String] = {
    MapOperation.MapTranspose(this.ReadStationInfoMap_latlonGetId1( ))
  }

  def ReadStationInfoMap_latlonGetId1 (): mutable.HashMap[String, String] = {
    val stationfile = PropertiesUtil.stationfile1
    val FileHeaderLines = 0
    val stnIdCols = 0
    val latCols = 2
    val lonCols = 1
    val splitStr = ","
    this.ReadStationInfoMap_latlonGetId(stationfile, FileHeaderLines, stnIdCols, latCols, lonCols, splitStr)
  }

  private def ReadStationInfo (stationfile: String): Array[String] = {
    if (this.checkStationOpen) {
      val Start = System.currentTimeMillis( )
      /*val sc = ContextUtil.getSparkContext()
      val stationsInfo: Array[String] = sc.textFile(stationfile).collect()*/

      val stationsInfo: Array[String] = HDFSReadWriteUtil.readTXT(stationfile)

      val End = System.currentTimeMillis( )
      val msg0 = ShowUtil.ArrayShowStr(stationsInfo)
      /*    val msg1=ws.showDateStr("ReadStationInfo:",Start,End)
          println(msg1+";"+msg0)*/

      WeatherShowtime.showDateStrOut1("ReadStationInfo:", Start, End, msg0)

      stationsInfo
    } else {
      null
    }
  }

  def ReadStationInfoMap_IdGetlatlon0 (): mutable.HashMap[String, String] = {
    if (this.checkStationOpen) {
      MapOperation.MapTranspose(this.ReadStationInfoMap_latlonGetId0( ))
    } else {
      null
    }
  }

  def ReadStationInfoMap_IdGetlatlon_stainfo_nat (): mutable.HashMap[String, String] = {
    if (this.checkStationOpen) {
      MapOperation.MapTranspose(this.ReadStationInfoMap_latlonGetId_stainfo_nat( ))
    } else {
      null
    }
  }

  def ReadStationInfoMap_latlonGetId_stainfo_nat (): mutable.HashMap[String, String] = {
    if (this.checkStationOpen) {
      val stationfile = PropertiesUtil.stationfile_stainfo_nat
      val FileHeaderLines = 0
      val stnIdCols = 0
      val latCols = 2
      val lonCols = 1
      val splitStr = ","
      this.ReadStationInfoMap_latlonGetId(stationfile, FileHeaderLines, stnIdCols, latCols, lonCols, splitStr)
    } else {
      null
    }
  }

  def analysisLatLon(latlon: String): (Float, Float) = {
    val lat = NumOperation.K3dec(latlon.split(this.latlonSplit)(0).toFloat)
    val lon = NumOperation.K3dec(latlon.split(this.latlonSplit)(1).toFloat)
    (lat, lon)
  }

  //  def main (args: Array[String]): Unit = {
  //    val ocfFileName = "D:\\Data\\forecast\\Input\\Ocf\\1h\\20180308\\06\\MSP3_PMSC_OCF1H_ME_L88_GLB_201803080600_00000-36000.DAT"
  //    val timeStep: Double = 1.0d
  //    val ocfMap = this.readOcf(ocfFileName, timeStep)
  //    println("ocfMap:" + ocfMap.size)
  //  }

  //消除对sc的依赖
  private def ReadOcf0(ocfFileName: String, timeStep: Double): Map[String, Map[String, Map[String, Float]]] = {
    val start = System.currentTimeMillis( )
    val ocfArr: Array[String] = {
      if (PropertiesUtil.isPrd) {
        HDFSReadWriteUtil.readTXT(ocfFileName)
      } else {
        ReadWrite.ReadTXT2ArrString(ocfFileName)
      }
    }
    var DateMap: TreeMap[String, Map[String, Float]] = TreeMap()
    var OcfMap: Map[String, Map[String, Map[String, Float]]] = Map()
    var lineNum: Int = 0
    var StaNum: Int = 0
    var Num: Int = 0
    var EleNum: Int = 0
    var id: String = null
    if (ocfArr.isEmpty) {
      val msg = "ocf数据读取后为空,ocfFileName:" + ocfFileName
      val e: Exception = new Exception(msg)
      e.printStackTrace( )
    } else {
      ocfArr.foreach(lineTxt => {
        val array = lineTxt.split("\\s++")
        val len = array.length
        if (lineNum < 3) {
          //读取文件头
          if (lineNum == 0) {
            StaNum = Integer.parseInt(array(1))
            EleNum = Integer.parseInt(array(4))
          }
          lineNum += 1
        } else { //读取数据
          if (len < 11) { //站号行
            id = array(0)
            DateMap = TreeMap( )
            Num += 1
          } else {
            var time = new String
            var eleMap: Map[String, Float] = Map() //需求要素
            var eleArray = new ArrayBuffer[String]( ) //全要素
            if (timeStep == 1.0d || timeStep == 3.0d) {
              //有11个要素
              if (!array(0).equals("")) {
                time = array(2)
                eleArray ++= array.slice(3, 14) //slice(from: Int, until: Int): List[A] 提取列表中从位置from到位置until(不含该位置)的元素列表
              } else {
                time = array(3)
                eleArray ++= array.slice(4, 15)
              }
            } else if (timeStep == 12.0d) {
              //有13个要素
              if (!array(0).equals("")) {
                time = array(1)
                eleArray ++= array.slice(2, 15)
              }
              else {
                time = array(2)
                eleArray ++= array.slice(3, 16)
              }
            }
            //在素有要素数组中提取需要的温度、降水和天气现象要素,通过列数确定要素对应ocf文件中的位置

            if (timeStep == 12.0d) {
              eleMap += (Constant.TEM + "_Max" -> eleArray(1).toFloat)
              eleMap += (Constant.TEM + "_Min" -> eleArray(2).toFloat)
            } else {
              eleMap += (Constant.TEM -> eleArray(0).toFloat)
            }

            eleMap += (Constant.PRE -> eleArray(3).toFloat)

            eleMap += (Constant.CLOUD -> eleArray(8).toFloat)

            eleMap += (Constant.WEATHER -> eleArray(9).toFloat)
            DateMap += (time.trim -> eleMap) //将需求的要素追加到对应的日期map中

            //  if(id.equals("58417")){
            if (OcfMap.contains(id)) {
              var tmpMap = OcfMap.get(id)
              OcfMap += (id -> DateMap)
            } else {
              OcfMap += (id -> DateMap)
            }

          }
        }
      })
    }
    val bak = "ocfMap:" + ocfFileName + " 最终输出大小：" + OcfMap.size
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("ReadOcfMap", start, end, bak)


    OcfMap
  }

  def ReadOcfFile1(ocfFileName: String, timeStep: Double, ocfdate: Date, appId: String): mutable.HashMap[(String, String, String), Float] = {
    val ocf: mutable.HashMap[(String, String, String), Float] = this.readOcf1(ocfFileName, timeStep)

    //-----------------Ocf数据采集日志接入-------------------------------
    //    val changePerson: String = "罗冰"
    //    //Ocf数据上游采集人员
    //    var basicRemark: JSONObject = new JSONObject
    //    basicRemark = AttributesOperation.addAsyOpen(basicRemark)
    //    basicRemark = AttributesOperation.addSourceDataType(basicRemark, Constant.OcfKey)
    //    basicRemark = AttributesOperation.addTimeStep(basicRemark, timeStep)
    //    basicRemark = AttributesOperation.addocffile(basicRemark, ocfFileName)
    //    basicRemark = AttributesOperation.addAppId(basicRemark, appId)
    //    val collectionflagStr: String = {
    //      if (ocf.isEmpty) {
    //        "N"
    //      } else {
    //        "Y"
    //      }
    //    }
    //    //    val sizeFlagStr:String="N"
    //    val sizeFlagStr: String = SendMsg.ocfsizeFlagStr(timeStep, ocfFileName)
    //    val arrFlagStr: String = SendMsg.ocfArrFlagStr(ocfdate, ocfFileName: String)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject
    //    stateRemark = AttributesOperation.addOtherMark(stateRemark, "Ocf数据源不做质控,暂不对文件大小做质控")
    //    SendMsg.collectionSend(ocfFileName, changePerson, basicRemark, collectionflagStr, sizeFlagStr, arrFlagStr, qualityFlagStr, stateRemark)
    //----------------------------------------------------------------------

    ocf
  }

  //  //<StnId,<time,<fcType,Value>>>
  //  private def ReadOcf0_before (ocfFileName: String, timeStep: Double): Map[String, Map[String, Map[String, Double]]] = {
  //    val sc = ContextUtil.getSparkContext( )
  //    val rdd: RDD[String] = sc.textFile(ocfFileName)
  //    var DateMap: TreeMap[String, Map[String, Double]] = TreeMap( )
  //    var OcfMap: Map[String, Map[String, Map[String, Double]]] = Map( )
  //    var lineNum: Int = 0
  //    var StaNum: Int = 0
  //    var Num: Int = 0
  //    var EleNum: Int = 0
  //    var id: String = null
  //    if (rdd.isEmpty( )) {
  //      val msg = "ocf数据读取后为空,ocfFileName:" + ocfFileName
  //      val e: Exception = new Exception(msg)
  //      e.printStackTrace( )
  //    } else {
  //      rdd.collect( ).foreach(lineTxt => {
  //        /*       i+=1
  //               println(lineTxt)*/
  //        val array = lineTxt.split("\\s++")
  //        val len = array.length
  //        if (lineNum < 3) {
  //          //读取文件头
  //          if (lineNum == 0) {
  //            StaNum = Integer.parseInt(array(1))
  //            EleNum = Integer.parseInt(array(4))
  //          }
  //          lineNum += 1
  //        } else { //读取数据
  //          if (len < 11) { //站号行
  //            id = array(0)
  //            DateMap = TreeMap( )
  //            Num += 1
  //          } else {
  //            var time = new String
  //            var eleMap: Map[String, Double] = Map( ) //需求要素
  //            var eleArray = new ArrayBuffer[String]( ) //全要素
  //            if (timeStep == 1.0d || timeStep == 3.0d) {
  //              //有11个要素
  //              if (!array(0).equals("")) {
  //                time = array(2)
  //                eleArray ++= array.slice(3, 14) //slice(from: Int, until: Int): List[A] 提取列表中从位置from到位置until(不含该位置)的元素列表
  //              } else {
  //                time = array(3)
  //                eleArray ++= array.slice(4, 15)
  //              }
  //            } else if (timeStep == 12.0d) {
  //              //有13个要素
  //              if (!array(0).equals("")) {
  //                time = array(1)
  //                eleArray ++= array.slice(2, 15)
  //              }
  //              else {
  //                time = array(2)
  //                eleArray ++= array.slice(3, 16)
  //              }
  //            }
  //            //在素有要素数组中提取需要的温度、降水和天气现象要素,通过列数确定要素对应ocf文件中的位置
  //
  //            if (timeStep == 12.0d) {
  //              eleMap += (Constant.TEM + "_Max" -> eleArray(1).toDouble)
  //              eleMap += (Constant.TEM + "_Min" -> eleArray(2).toDouble)
  //            } else {
  //              eleMap += (Constant.TEM -> eleArray(0).toDouble)
  //            }
  //
  //
  //            eleMap += (Constant.PRE -> eleArray(3).toDouble)
  //            eleMap += (Constant.WEATHER -> eleArray(9).toDouble)
  //            DateMap += (time.trim -> eleMap) //将需求的要素追加到对应的日期map中
  //
  //            //  if(id.equals("58417")){
  //            if (OcfMap.contains(id)) {
  //              var tmpMap = OcfMap.get(id)
  //              OcfMap += (id -> DateMap)
  //            } else {
  //              OcfMap += (id -> DateMap)
  //            }
  //
  //          }
  //        }
  //      })
  //    }
  //
  //    println("ocfMap:" + ocfFileName + " 最终输出大小：" + OcfMap.size)
  //    OcfMap
  //  }

  def readOcf1(ocfFileName: String, timeStep: Double): mutable.HashMap[(String, String, String), Float] = {
    this.ReadOcf1(ocfFileName, timeStep)
  }

  //[(StnId,time,fcType),Value]
  private def ReadOcf1(ocfFileName: String, timeStep: Double): mutable.HashMap[(String, String, String), Float] = {
    val start = System.currentTimeMillis( )
    val ocfArr: Array[String] = {
      if (PropertiesUtil.isPrd) {
        HDFSReadWriteUtil.readTXT(ocfFileName)
      } else {
        ReadWrite.ReadTXT2ArrString(ocfFileName)
      }
    }
    val OcfMap: mutable.HashMap[(String, String, String), Float] = new mutable.HashMap[(String, String, String), Float]
    var lineNum: Int = 0
    var StaNum: Int = 0
    var Num: Int = 0
    var EleNum: Int = 0
    var id: String = null
    val eles: Array[(String, Int)] = {
      if (timeStep == 1.0d) {
        Array((Constant.PRE, 3), (Constant.TEM, 0), (Constant.WEATHER, 9))
      } else if (timeStep == 12.0d) {
        Array((Constant.PRE, 3), (Constant.TEM + "_Max", 1), (Constant.TEM + "_Min", 2), (Constant.WEATHER, 9))
      } else {
        Array( )
      }
    }
    if (ocfArr.isEmpty) {
      val msg = "ocf数据读取后为空,ocfFileName:" + ocfFileName
      val e: Exception = new Exception(msg)
      e.printStackTrace( )
    } else {
      ocfArr.foreach(lineTxt => {
        /*       i+=1
               println(lineTxt)*/
        val array = lineTxt.split("\\s++")
        val len = array.length
        if (lineNum < 3) {
          //读取文件头
          if (lineNum == 0) {
            StaNum = Integer.parseInt(array(1))
            EleNum = Integer.parseInt(array(4))
          }
          lineNum += 1
        } else { //读取数据
          if (len < 11) { //站号行
            id = array(0)
            Num += 1
          } else {
            val (eleArray: ArrayBuffer[String], time: String) = {
              var eleArray0 = new ArrayBuffer[String]( ) //全要素
              var time0 = new String
              if (timeStep == 1.0d || timeStep == 3.0d) {
                //有11个要素
                if (!array(0).equals("")) {
                  time0 = array(2)
                  eleArray0 ++= array.slice(3, 14) //slice(from: Int, until: Int): List[A] 提取列表中从位置from到位置until(不含该位置)的元素列表
                } else {
                  time0 = array(3)
                  eleArray0 ++= array.slice(4, 15)
                }
              } else if (timeStep == 12.0d) {
                //有13个要素
                if (!array(0).equals("")) {
                  time0 = array(1)
                  eleArray0 ++= array.slice(2, 15)
                } else {
                  time0 = array(2)
                  eleArray0 ++= array.slice(3, 16)
                }
              }
              (eleArray0, time0.trim)
            }
            eles.foreach(ele => {
              OcfMap.put((id, time, ele._1), eleArray(ele._2).toFloat)
            })

          }
        }
      })
    }
    val bak = "ocfMap1:" + ocfFileName + " 最终输出大小：" + OcfMap.size
    val end = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("ReadOcfMap", start, end, bak)


    OcfMap
  }

  private def ReadTemExFile_Before(TemExFile: String): Map[String, Map[String, Array[Double]]] = {
    //    println("ReadTemExFile="+path)
    val sc = ContextUtil.getSparkContext()
    val rdd: RDD[String] = sc.textFile(TemExFile)
    val length = 12
    var TemExMap: Map[String, Map[String, Array[Double]]] = Map()
    var ExTypeMap: Map[String, Array[Double]] = Map()
    var lineNum = 0
    rdd.collect().foreach(lineTxt => {
      lineNum += 1
      var array = lineTxt.split(",")
      var areaName = array(0)
      var exType = array(1)
      var exTemp = new Array[Double](length)
      for (i <- 0 until length) {
        exTemp(i) = array(i + 2).toDouble
      }
      if (lineNum % 2 == 1) {
        ExTypeMap = Map()
      }
      ExTypeMap += (exType -> exTemp)
      if (TemExMap.contains(areaName)) {
        var tmpMap = TemExMap.get(areaName)
        TemExMap += (areaName -> ExTypeMap)
      } else {
        TemExMap += (areaName -> ExTypeMap)
      }

    })

    println("ReadTemExFile=" + TemExFile + ",TemExMap 最终输出大小：" + TemExMap.size)
    TemExMap


  }

  private def ReadStationInfo(): Array[String] = {
    val stationfile = PropertiesUtil.stationfile0
    this.ReadStationInfo(stationfile)
  }


}
