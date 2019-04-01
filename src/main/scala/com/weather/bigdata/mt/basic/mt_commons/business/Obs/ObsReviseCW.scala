package com.weather.bigdata.mt.basic.mt_commons.business.Obs

import java.util.Date

import scala.collection.mutable

object ObsReviseCW {
  def mainobs(splitFile:String, obsdate:Date, fcTypes_obs:Array[String], timeSteps:Array[Double], Obsfile:String, stationInfos:(mutable.HashMap[String,String],String,Date)): Boolean ={
    ObsReviseCW0.mainobs(splitFile,obsdate,fcTypes_obs,timeSteps,Obsfile,stationInfos)
  }

}
