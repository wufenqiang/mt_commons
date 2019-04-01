package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil

import java.io.FileWriter
import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation.{ArrayOperation, DateOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CheckStationCW {
  val SplitSpace = "          "
  //  private val AO:ArrayOperation=new ArrayOperation
  private val DateOp: DateOperation = new DateOperation
  //  private val rw=HDFSReadWriteUtil
  private val checkStationOpen0: Boolean = {
    val flag = PropertiesUtil.checkStationOpen
    if (!flag) {
      println("CheckStation总开关checkStationOpen0关闭")
    }
    flag
  }

  def checkStation(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, rdd: RDD[SciDatasetCW], addressdate: Date, showDate: Date): RDD[SciDatasetCW] = {
    rdd.map(
      scidata => {
        this.checkStation(appID, StnIdGetLatLon, checkOutRootPath, scidata, addressdate, showDate)
      }
    )
  }

  private def filewriteOp(out: FileWriter, str: String, splitSpace: String): Unit = {
    this.filewriteOp(out, Array(str), splitSpace)
  }

  private def checkStation(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, rdd: RDD[SciDatasetCW]): RDD[SciDatasetCW] = {
    rdd.map(
      scidata => {
        val fcdate = AttributesOperationCW.getfcdate(scidata)
        this.checkStation(appID, StnIdGetLatLon, checkOutRootPath, scidata, fcdate, fcdate)
      }
    )
  }

  private def checkStation(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, scidata: SciDatasetCW): SciDatasetCW = {
    val fcdate = AttributesOperationCW.getfcdate(scidata)
    this.checkStation(appID, StnIdGetLatLon, checkOutRootPath, scidata, fcdate, fcdate)
  }

  private def checkStation_Action(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, rdd: RDD[(Date, SciDatasetCW)]): Unit = {
    rdd.foreach(
      f => {
        val scidata = f._2
        val date = f._1
        this.checkStation(appID, StnIdGetLatLon, checkOutRootPath, scidata, date, date)
      }
    )
  }

  private def checkStation_Return(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, rdd: RDD[(Date, SciDatasetCW)]): RDD[SciDatasetCW] = {
    rdd.map(
      f => {
        val scidata = f._2
        val date = f._1
        this.checkStation(appID, StnIdGetLatLon, checkOutRootPath, scidata, date, date)
      }
    )
  }

  def checkStation(appID: String, StnIdGetLatLon: mutable.HashMap[String, String], checkOutRootPath: String, scidata: SciDatasetCW, addressdate: Date, begindate: Date): SciDatasetCW = {
    if (this.checkStationOpen0) {
      try {
        val startTime = System.currentTimeMillis()
        val idName = AttributesOperationCW.getIDName(scidata)
        val fcType: String = AttributesOperationCW.getFcType(scidata)
        val timeStep: Double = AttributesOperationCW.gettimeStep(scidata)
        val checkOutPath = PropertiesUtil.getcheckOutPath(checkOutRootPath, timeStep, fcType, addressdate)

        //        val groupKeyStr=MatchSciDatas.idNameTimeStep2KeyShowStr(idName,timeStep)

        val tmpPath = PropertiesUtil.getWriteTmp
        val fileName = scidata.datasetName.replace(".nc", ".txt").replace(appID, "")
        val tmpfileName = tmpPath + fileName
        val checkOutfile = checkOutPath + fileName
        if (HDFSOperation1.exists(tmpfileName)) {
          val msg = "checkStation写文件时,存在本地中转文件,删除本地中转文件:" + tmpfileName
          println(msg)
          HDFSOperation1.delete(tmpfileName)
        }


        val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()

        val dataNames = scidata.variables.keySet.-(timeName, heightName, latName, lonName).toArray

        val fileHead = ArrayOperation.addStr(Array("LST"), dataNames)

        val timeV: VariableCW = scidata.apply(timeName)
        val heightV: VariableCW = scidata.apply(heightName)
        val latV: VariableCW = scidata.apply(latName)
        val lonV: VariableCW = scidata.apply(lonName)

        //        val time: Array[Double] = scidata.apply(timeName).dataDouble()
        //        val height:Array[Double]=scidata.apply(heightName).dataDouble()
        //        val lat: Array[Double] = NumOperation.K3dec(scidata.apply(latName).dataDouble)
        //        val lon: Array[Double] = NumOperation.K3dec(scidata.apply(lonName).dataDouble)
        val datas: Array[VariableCW] = dataNames.map(dataName => scidata.apply(dataName))

        val timeLen = timeV.getLen()
        val heightLen = heightV.getLen()
        val latLen = latV.getLen()
        val lonLen = lonV.getLen()

        val lat: Array[Float] = NumOperation.K3dec(latV.dataFloat())
        val lon: Array[Float] = NumOperation.K3dec(lonV.dataFloat())

        val beginLat = lat.min
        val endLat = lat.max
        val beginLon = lon.min
        val endLon = lon.max

        StnIdGetLatLon.foreach(kv => {
          val station_id = kv._1
          val LatLon = kv._2
          val (station_lat: Float, station_lon: Float) = ReadFile.analysisLatLon(LatLon)

          if (beginLat <= station_lat && station_lat <= endLat && beginLon <= station_lon && station_lon <= endLon) {
            val latIndex: Int = ArrayOperation.nearIndex_sequence(lat, station_lat)
            val lonIndex: Int = ArrayOperation.nearIndex_sequence(lon, station_lon)

            val lat0 = lat(latIndex)
            val lon0 = lon(lonIndex)

            println("fcType=" + fcType + ",NameID=" + idName + ",timgStep=" + timeStep + ";存在抽站点,stnId=" + station_id + ";lat=" + station_lat + ",lon=" + station_lon + ";latIndex=" + latIndex + ",lonIndex=" + lonIndex)

            if (!HDFSOperation1.exists(tmpfileName)) {
              HDFSOperation1.createFile(tmpfileName)
              println("创建tmpfileName=" + tmpfileName)
              val out = new FileWriter(tmpfileName, false)
              this.filewriteOp(out, fileHead, SplitSpace)
              out.close()
            }
            val out = new FileWriter(tmpfileName, true)
            this.filewriteOp(out, Array(station_id, ("station_lat=" + station_lat + ",station_lon=" + station_lon), ("latIndex=" + latIndex + ",lonIndex=" + lonIndex), ("lat=" + lat0 + ",lon=" + lon0)), SplitSpace)

            val heightIndex: Int = {
              if (heightLen != 1) {
                println(fcType + "数据存在高度值,取默认高度层号为0位置")
              }
              0
            }

            for (timeIndex <- 0 to timeLen - 1) {
              val line = new Array[String](dataNames.length + 1)
              val index = IndexOperation.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, heightLen, latLen, lonLen)

              val theDate = DateOp.plusHour(begindate, (timeIndex + 1) * timeStep.toInt)
              var dataValue: Double = -1
              line(0) = DateFormatUtil.YYYYMMDDHHStr1(theDate)
              for (j <- 0 to dataNames.length - 1) {
                dataValue = NumOperation.K3dec(datas(j).dataDouble(index))
                line(j + 1) = dataValue.toString
              }
              this.filewriteOp(out, line, SplitSpace)
            }
            if(HDFSOperation1.exists(tmpfileName)){
              out.close()
            }
          }

        })
        if (HDFSOperation1.exists(tmpfileName)) {
          val copy = HDFSOperation1.copyfile(tmpfileName, checkOutPath, true, true)
          if (copy) {
            println("该区域存在检测点timeStep=" + timeStep + ",NameID=" + idName + ",checkOutfile=" + checkOutfile)
          } else {
            println("该区域存在检测点timeStep=" + timeStep + ",NameID=" + idName + ",但拷贝上HDFS没有成功,tmpfileName=" + tmpfileName + ",checkOutfile=" + checkOutfile)
          }
        } else {
          println("该区域不存在检测点timeStep=" + timeStep + ",NameID=" + idName)
        }

        val endTime = System.currentTimeMillis()
        //        //--------------------日志采集----------------------------------

        //        val fileList=""
        //        var basicRemark:JSONObject=new JSONObject()
        //        basicRemark=AttributesOperationCW.addFcType(basicRemark,fcType)
        //        basicRemark=AttributesOperationCW.addIDName(basicRemark,idName)
        //        basicRemark=AttributesOperationCW.addAppId(basicRemark,appID)
        //        basicRemark=AttributesOperationCW.addbasicProcess(basicRemark,"抽站点"+this.getClass.getName)
        //        basicRemark=AttributesOperationCW.addOtherMark(basicRemark,"站点数:"+StnIdGetLatLon.size)
        //        val processFlagStr:String={
        //          if(StnIdGetLatLon.isEmpty){
        //            "N"
        //          }else{
        //            "Y"
        //          }
        //        }
        //        val qualityFlagStr:String="N"
        //        var stateRemark:JSONObject=new JSONObject()
        //        stateRemark=AttributesOperationCW.addOtherMark(stateRemark,"checkOutfile:"+checkOutfile)
        //        val stateDetailRemark:JSONObject=new JSONObject()
        //        SendMsgCW.processSend(fileList,startTime,endTime,basicRemark,processFlagStr,qualityFlagStr,stateRemark,stateDetailRemark)
        //        //--------------------------------------------------------------
        WeatherShowtime.showDateStrOut1("checkStation:", startTime, endTime, "idName=" + idName + ",fcType=" + fcType + ",timeStep=" + timeStep)
      } catch {
        case e: Exception => {
          println("抽站失败" + e.toString)
        }
      }

    } else {
      val msg = "checkStationOpen0=" + checkStationOpen0 + ",抽站总开关关闭." + this.getClass.getCanonicalName
      println(msg)
    }
    scidata
  }

  private def filewriteOp(out: FileWriter, arrStr: Array[String], splitSpace: String): Unit = {
    for (i <- 0 to arrStr.length - 1) {
      out.write(arrStr(i))
      out.write(splitSpace)
    }
    out.write("\n")
  }

  /*def main(args: Array[String]): Unit = {
    println("--------------------------------------------------------------Start:args.length="+args.length+"--------------------------------------------------------------")
    ShowUtil.ArrayShowInfo(args)

    val appID=ContextUtil.getApplicationID
    val jsonFile=PropertiesUtil.getjsonFile()
    val jsonRdd=PropertiesUtil.getJsonRdd(jsonFile)

    val dateOp=new dateOperation

    val stationfile1=PropertiesUtil.getstationfile_stainfo_nat
    //    val stationsInfo2: Array[String] = ReadFile.ReadStationInfo(stationfile1)
    val FileHeaderLines=0
    val stnIdCols=0
    val latCols=2
    val lonCols=1
    val splitStr=","
    val StnIdGetLatLon2=ReadFile.ReadStationInfoMap_IdGetlatlon(stationfile1,FileHeaderLines,stnIdCols,latCols,lonCols,splitStr)

    val (datestart:Date,dateend:Date,fcTypes:Array[String],timeSteps:Array[Double])={
      if(args.length==1) {
        val Datestart:Date =DataFormatUtil.YYYYMMDDHHMMSS0(args(0))
        val Dateend=Datestart
        val FcTypes=Array(ConstantUtil.PRE,ConstantUtil.CLOUD,ConstantUtil.TEM,ConstantUtil.RHU,ConstantUtil.WIND,ConstantUtil.WEATHER)
        val TimeSteps=Array(1.0d,12.0d)
        (Datestart, Dateend, FcTypes, TimeSteps)
      }else if(args.length==2){
        val Datestart:Date =DataFormatUtil.YYYYMMDDHHMMSS0(args(0))
        val Dateend:Date =DataFormatUtil.YYYYMMDDHHMMSS0(args(1))
        val FcTypes=Array(ConstantUtil.PRE,ConstantUtil.CLOUD,ConstantUtil.TEM,ConstantUtil.RHU,ConstantUtil.WIND,ConstantUtil.WEATHER)
        val TimeSteps=Array(1.0d,12.0d)
        (Datestart, Dateend, FcTypes, TimeSteps)
      }else if(args.length==3) {
        val Datestart: Date = DataFormatUtil.YYYYMMDDHHMMSS0(args(0))
        val Dateend: Date = DataFormatUtil.YYYYMMDDHHMMSS0(args(1))
        val FcTypes = Array(ConstantUtil.PRE, ConstantUtil.CLOUD, ConstantUtil.TEM, ConstantUtil.RHU, ConstantUtil.WIND, ConstantUtil.WEATHER)
        val TimeStep = args(2).toDouble
        val TimeSteps = Array(TimeStep)
        (Datestart, Dateend, FcTypes, TimeSteps)
      }else if(args.length==4){
        val Datestart: Date = DataFormatUtil.YYYYMMDDHHMMSS0(args(0))
        val Dateend: Date = DataFormatUtil.YYYYMMDDHHMMSS0(args(1))
        val TimeStep = args(2).toDouble
        val FcType =args(3)
        val FcTypes = Array(FcType)
        val TimeSteps = Array(TimeStep)
        (Datestart, Dateend, FcTypes, TimeSteps)
      }else{
        null
      }
    }

    val checkOutRootPath2 =PropertiesUtil.checkoutRootPath2

    val dates=dateOp.dateArray(datestart,dateend,12.0d)
    val rdds:Array[RDD[(Date,SciDataset)]]=dates.map(
      date=>{
        fcTypes.map(
          fcType=>{
            timeSteps.map(
              timeStep=>{
                jsonRdd.map(
                  json=>{
                    val (idName,timeBegin,timeEnd,timeStep0,heightBegin,heightEnd,heightStep,latBegin,latEnd,latStep,lonBegin,lonEnd,lonStep)=AccordSciDatas_JsonObject.analysisJsonProperties(json)
                    val dateStr=DataFormatUtil.YYYYMMDDHHMMSSStr0(date)

                    val flag:Boolean={
                      StnIdGetLatLon2.map(
                        kv=>{
                          val LatLon=kv._2
                          val (station_lat,station_lon)=ReadFile.analysisLatLon(LatLon)

                          if ((latBegin<= station_lat) && (station_lat <= latEnd) && (lonBegin<=station_lon) && (station_lon <= lonEnd)) {
                            val msg0="idName="+idName+"存在检测点,读取相应scidata"
                            println(msg0)
                            true
                          }else{
                            false
                          }
                        }
                      ).reduce((x,y)=>(x || y))
                    }
                    if(flag){
                      if(ReadFile.ReadFcExist(idName,timeStep,fcType,date)){
                        (date,ReadFile.ReadFcScidata(idName,timeStep,fcType,date))
                      }else{
                        val msg="源数据异常;idName="+idName+";timeStep="+timeStep+";fcType="+fcType+";date="+dateStr
                        println(msg)
                        null
                      }
                    }else{
                      val msg="idName="+idName+",不存在站点"
                      println(msg)
                      null
                    }
                  }
                ).filter(f=>(f!=null))
              }
            ).reduce((x,y)=>(x.union(y))).filter(f=>(f!=null))
          }
        ).reduce((x,y)=>(x.union(y))).filter(f=>(f!=null))
      }
    )
    rdds.foreach(rdd=>{
      this.checkStation_Action(appID,StnIdGetLatLon2,checkOutRootPath2,rdd)
    })
  }*/
}
