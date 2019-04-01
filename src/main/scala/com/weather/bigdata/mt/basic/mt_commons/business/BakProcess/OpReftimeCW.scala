package com.weather.bigdata.mt.basic.mt_commons.business.BakProcess

import java.util.Date

import com.weather.bigdata.it.utils.ShowUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}

import scala.collection.mutable

object OpReftimeCW {
  private val changeRFCover = PropertiesUtil.changeRFCover
  private val changeRFCoverAll = PropertiesUtil.changeRFAllCover

  def changereftime(jsonFile: String, fcTypes: Array[String], fcdatenew: Date, timeSteps: Array[Double], fcdateold: Date, stationsInfos: (mutable.HashMap[String, String], String, Date),changeRFCover:Boolean, changeRFCoverAll:Boolean): Boolean = {
    //    ChangeReftime.changereftime(jsonFile, fcTypes, fcdatenew, timeSteps, fcdateold, stationsInfos, changeRFCover, changeRFCoverAll)
    ChangeReftimeCW1.changereftime(jsonFile,fcTypes,fcdatenew,timeSteps,fcdateold,stationsInfos,changeRFCover:Boolean, changeRFCoverAll:Boolean)
  }
  def main(args: Array[String]): Unit = {
    val fcTypes = Constant.FcTypes_changeRefTime
    val timeSteps = Constant.TimeSteps

    ShowUtil.ArrayShowInfo(args)

    val (fcdateold: Date, fcdatenew: Date, jsonFile: String) = {
      val jsonFile = args(0)
      val now = {
        //        if(args.length>1){
        //          DateFormatUtil.YYYYMMDDHHMMSS0(args(1))
        //        }else{
        new Date()
        //        }
      }
      val fcdatenew = WeatherDate.nextFCDate(now)
      val fcdateold = WeatherDate.previousFCDate(now)
      (fcdateold, fcdatenew, jsonFile)
    }


    // /站点数据
    val StnIdGetLatLon: mutable.HashMap[String, String] = ReadFile.ReadStationInfoMap_IdGetlatlon_stainfo_nat
    val stationchangeReftime = PropertiesUtil.stationchangeReftime
    val stationInfos = (StnIdGetLatLon, stationchangeReftime, fcdatenew)


    //    val jsonFile=PropertiesUtil.getjsonFile()
    //    val jsonRdd=PropertiesUtil.getJsonRdd(jsonFile)
    this.changereftime(jsonFile, fcTypes, fcdatenew, timeSteps, fcdateold, stationInfos,this.changeRFCover, this.changeRFCoverAll)
  }
}
