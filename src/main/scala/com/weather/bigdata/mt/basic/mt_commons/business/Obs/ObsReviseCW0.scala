package com.weather.bigdata.mt.basic.mt_commons.business.Obs

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.it.utils.{ShowUtil, WeatherShowtime}
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.CopyRddUtil.CpRddCW
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Replace.DataReplaceCW
import com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil.TimeInterpolationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.{ReadFile, ReadNcCW, ReadObsCW, WriteNcCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil.NcRevisedByObsCW
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object ObsReviseCW0 {

  def main(args: Array[String]): Unit = {
    val ObsdateStr: String = args(0)
    val Obsdate: Date = DateFormatUtil.YYYYMMDDHHMMSS0(ObsdateStr)
    //    val timeSteps=Array(1.0d,12.0d)
    val timeSteps = Constant.TimeSteps
    val ObsFile = PropertiesUtil.getobsFile(Obsdate)
    // /站点数据
    val stationInfo: mutable.HashMap[String, String] = ReadFile.ReadStationInfoMap_IdGetlatlon0()
    val jsonFile = PropertiesUtil.getjsonFile()
    val stationobs = PropertiesUtil.stationobs
    val stationInfos = (stationInfo, stationobs, Obsdate)
    val appId: String = ContextUtil.getApplicationID
    if (args.length == 1) {
      val fcTypes = Constant.FcTypes_obs
      val flag = this.mainobs(jsonFile, Obsdate, fcTypes, timeSteps, ObsFile, stationInfos)
      if (flag) {
        println("ObsRevise:TEM、PRE、WEATHER计算完成。")
      }
    } else {
      val fcType: String = args(1)
      this.mainobs(jsonFile, Obsdate, Array(fcType), timeSteps, ObsFile, stationInfos)
    }

  }

  def mainobs(splitFile: String, obsdate: Date, fcTypes: Array[String], timeSteps: Array[Double], Obsfile: String, stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val appId = ContextUtil.getApplicationID
    val fcTypes_excWEATHER = fcTypes.filter(p => (!p.equals(Constant.WEATHER)))
    val flag: Boolean = {
      val obsdatas: SciDatasetCW = ReadObsCW.ReadObs(Obsfile, fcTypes_excWEATHER)
      this.parallelobs(splitFile, obsdate, fcTypes, timeSteps, obsdatas, stationInfos)
    }
    flag
  }

  private def parallelobs(splitFile: String, obsdate: Date, fcTypes: Array[String], timeSteps: Array[Double], obsdatas: SciDatasetCW, stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val start = System.currentTimeMillis()

    val FcTypes_expWea: Array[String] = fcTypes.filter(p => (!p.equals(Constant.WEATHER)))
    val rdd1: RDD[SciDatasetCW] = this.obsrevise1(splitFile, obsdate, FcTypes_expWea, obsdatas, stationInfos)

    val timeSteps_exp1 = timeSteps.filter(p => p != 1.0d)
    val obsflag: Boolean = this.obsrevise123(obsdate, rdd1, FcTypes_expWea, timeSteps_exp1, stationInfos)
    val end = System.currentTimeMillis()


    WeatherShowtime.showDateStrOut1("ObsRevise:", start, end, ShowUtil.ArrayShowStr(FcTypes_expWea) + ";" + ShowUtil.ArrayShowStr(timeSteps))


    val End1 = System.currentTimeMillis()
    WeatherShowtime.showDateStrOut1("ObsRevise:", start, End1, "PRE,TEM")
    obsflag
  }

  private def obsrevise1(splitFile: String, obsdate: Date, fcTypes: Array[String], obsData: SciDatasetCW, stationInfos: (mutable.HashMap[String, String], String, Date)): RDD[SciDatasetCW] = {
    val start = System.currentTimeMillis()
    val sc = ContextUtil.getSparkContext()
    val appID = ContextUtil.getApplicationID
    val fcdate = WeatherDate.ObsGetFc(obsdate)
    val timeStep: Double = 1.0d
    val obsdateStr: String = DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)
    val fcdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(fcdate)

    val para: String = "fcdateStr=" + fcdateStr + ";obsdateStr=" + obsdateStr + ";timeStep=" + timeStep

    val latName_Obs = PropertiesUtil.latName_Obs
    val lonName_Obs = PropertiesUtil.lonName_Obs

    val obsDataValue = sc.broadcast(obsData)

    val fcRdd1s: RDD[SciDatasetCW] = Input1hRddCW.input1hRdd(splitFile, fcTypes, obsdate)

    val obsRdd1: RDD[SciDatasetCW] = fcRdd1s.map(
      scidata => {
        val startTime = System.currentTimeMillis()

        val fcType: String = AttributesOperationCW.getFcType(scidata)
        val idName: String = AttributesOperationCW.getIDName(scidata)
        val dataName_Obs = PropertiesUtil.getObsDataName(fcType)

        val wdataName = PropertiesUtil.getwElement(fcType, timeStep)(0)

        var obsdata1: SciDatasetCW = NcRevisedByObsCW.Obs(scidata, obsDataValue.value, wdataName, latName_Obs, lonName_Obs, dataName_Obs, obsdate)
        obsdata1 = AttributesOperationCW.addTrace(obsdata1, "obsrevise1[" + DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate) + "]")

        //--------------------日志采集----------------------------------
        //        val endTime=System.currentTimeMillis()
        //        val fileList=""
        //        var basicRemark:JSONObject=new JSONObject()
        //        basicRemark=AttributesOperationCW.addFcType(basicRemark,fcType)
        //        basicRemark=AttributesOperationCW.addIDName(basicRemark,idName)
        //        basicRemark=AttributesOperationCW.addAppId(basicRemark,appID)
        //        val processFlagStr:String=SendMsgCW.flagStr_Scidata(obsdata1)
        //        val qualityFlagStr:String="N"
        //        var stateRemark:JSONObject=new JSONObject()
        //        stateRemark=AttributesOperationCW.addOtherMark(stateRemark,"ObsRevise1")
        //        val stateDetailRemark:JSONObject=new JSONObject()
        //        SendMsg.processSend(fileList,startTime,endTime,basicRemark,processFlagStr,qualityFlagStr,stateRemark,stateDetailRemark)
        //--------------------------------------------------------------

        //抽取站点
        val stationInfo = stationInfos._1
        val stationobs = stationInfos._2
        val addresssdate = stationInfos._3
        CheckStationCW.checkStation(appID, stationInfo, stationobs, obsdata1, addresssdate, fcdate)

        val flag = WriteNcCW.WriteFcFile_addId_tmp(obsdata1, appID)


        obsdata1
      }
    )

    obsDataValue.unpersist()
    /*val (jsonPro:RDD[JSONObject],splitNum:Int)=PropertiesUtil.getJsonRdd
    val obsflag1:Boolean=fcTypes.map(
      fcType=>{
        val dataName_Obs=PropertiesUtil.getObsDataName(fcType)
        val vars:Array[String]=Array(latName_Obs,lonName_Obs,dataName_Obs)
        val obsdata=ReadFile.ReadNcScidata(ObsFileName,vars)
        val fctypeflag:Boolean=jsonPro.map(
          jpo=>{
            val idName:String=jpo.getString(AttributesOperation.idNameKey)

            val FcNcFile1=PropertiesUtil.getfcFile(timeStep,fcType,fcdate,idName)
            val fcNcPath1=PropertiesUtil.getfcPath(timeStep,fcType,fcdate)

            println("FcNcFile1="+FcNcFile1)
            val scidata=ReadFile.ReadNcScidata(FcNcFile1)

//            val AfterObs1FcNcFile=PropertiesUtil.getAfterObsfcPath(timeStep,fcType,fcdate)

            val wdataName=PropertiesUtil.getwElement(fcType,timeStep)(0)

            var obsdata1:SciDataset= Revise.Obs(scidata,obsdata,fcType ,timeName,heightName,latName,lonName,wdataName,latName_Obs,lonName_Obs,dataName_Obs,fcdate,obsdate)

            obsdata1=AttributesOperation.addAppIDTrace(obsdata1,appID)
            val flag=WriteFile.WriteNcFile_addID_tmp(obsdata1,fcNcPath1,appID)

            if(flag){
              println("obsrevise1生产nc数据到临时路径成功")
            }else{
              val e:Exception=new Exception(idName+"obsrevise1生产nc数据到临时路径失败")
              e.printStackTrace()
            }
            flag
          }
        ).reduce((x,y)=>(x && y))
        WriteFile.WriteNcFile_addID_tmp2path(fctypeflag,timeStep,fcTypes,appID,fcdate)
        fctypeflag
      }
    ).reduce((x,y)=>(x && y))*/
    val end = System.currentTimeMillis()
    WeatherShowtime.showDateStrOut1("obsRevise0.obsrevise1:", start, end, para)

    obsRdd1
  }

  private def obsrevise123(obsdate: Date, obsRdd1: RDD[SciDatasetCW], fcTypes: Array[String], timeSteps: Array[Double], stationInfos: (mutable.HashMap[String, String], String, Date)): Boolean = {
    val start = System.currentTimeMillis()

    val appID = ContextUtil.getApplicationID
    val fcdate = WeatherDate.ObsGetFc(obsdate)

    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()
    val varsName = (timeName, heightName, latName, lonName)

    val cprdd: RDD[SciDatasetCW] = CpRddCW.cprddonlytimeStep(obsRdd1, timeSteps)
    val flag: Boolean = cprdd.map(
      scidata => {
        val startTime = System.currentTimeMillis()

        val idName = AttributesOperationCW.getIDName(scidata)
        val timeStep = AttributesOperationCW.gettimeStep(scidata)
        val fcType = AttributesOperationCW.getFcType(scidata)

        val scidata123: SciDatasetCW = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdate)
        val dataNames_alTypetimeIAs: Array[(String, String)] = PropertiesUtil.getdataNames_alTypetimeIAs_from1h(fcType, timeStep)
        val times123 = ArrayOperation.ArithmeticArray(timeStep, 72.0d, timeStep)
        val obsrevise_little = TimeInterpolationCW.timeIA(scidata, varsName, dataNames_alTypetimeIAs, times123)
        val wdataName123 = PropertiesUtil.getwElement(fcType, timeStep)

        //++++++++++++++++++++测试代码+++++++++++++++++++++++
        /*val test0Path=PropertiesUtil.gethdfsWriteTmp()+"obsrevise_little/"
        val test1Path=PropertiesUtil.gethdfsWriteTmp()+"scidata123/"
        WriteFile.WriteNcFile_test(obsrevise_little,test0Path)
        WriteFile.WriteNcFile_test(scidata123,test1Path)*/
        //++++++++++++++++++++++++++++++++++++++++++++++++++


        var obsdata123 = DataReplaceCW.dataReplace((obsrevise_little, scidata123), timeName, heightName, latName, lonName, wdataName123)
        obsdata123 = AttributesOperationCW.addAppIDTrace(obsdata123, appID)
        obsdata123 = AttributesOperationCW.addTrace(obsdata123, "ObsRevise123[" + DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate) + "]")


        //--------------------日志采集----------------------------------
        //        val endTime=System.currentTimeMillis()
        //        val fileList=""
        //        var basicRemark:JSONObject=new JSONObject()
        //        basicRemark=AttributesOperationCW.addFcType(basicRemark,fcType)
        //        basicRemark=AttributesOperationCW.addIDName(basicRemark,idName)
        //        basicRemark=AttributesOperationCW.addAppId(basicRemark,appID)
        //        val processFlagStr:String=SendMsgCW.flagStr_Scidata(obsdata123)
        //        val qualityFlagStr:String="N"
        //        var stateRemark:JSONObject=new JSONObject()
        //        stateRemark=AttributesOperationCW.addOtherMark(stateRemark,"ObsRevise123")
        //        val stateDetailRemark:JSONObject=new JSONObject()
        //        SendMsg.processSend(fileList,startTime,endTime,basicRemark,processFlagStr,qualityFlagStr,stateRemark,stateDetailRemark)
        //--------------------------------------------------------------


        val flag = WriteNcCW.WriteFcFile_addId_tmp(obsdata123, appID)

        if (flag) {
          //抽取站点
          val stationInfo = stationInfos._1
          val stationobs = stationInfos._2
          CheckStationCW.checkStation(appID, stationInfo, stationobs, obsdata123, obsdate, fcdate)
          obsdata123
        }

        flag
      }
    ).reduce((x, y) => (x && y))

    val end = System.currentTimeMillis()
    WeatherShowtime.showDateStrOut1("obsRevise12_3:", start, end, ShowUtil.ArrayShowStr(fcTypes) + ";" + ShowUtil.ArrayShowStr(timeSteps))
    flag
  }
}
