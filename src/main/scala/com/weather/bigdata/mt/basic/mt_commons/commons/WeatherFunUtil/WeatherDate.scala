package com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil

import java.text.SimpleDateFormat
import java.util.Date

import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.DateOperation

import scala.collection.mutable
class WeatherDate {
  private val sdfYMD =new SimpleDateFormat("yyyyMMdd")
  private val sdfYMDH = new SimpleDateFormat("yyyyMMddHH")
  private val sdfYMD06 = new SimpleDateFormat("yyyyMMdd06")
  private val sdfYMD08 = new SimpleDateFormat("yyyyMMdd08")
  private val sdfYMD12 = new SimpleDateFormat("yyyyMMdd12")
  private val sdfYMD20 = new SimpleDateFormat("yyyyMMdd20")
  private val dateOp = new DateOperation

  def ObsGetKVDateStr_Old(obsdate:Date,timeStep:Double):String={
    val fcdate=this.ObsGetFc(obsdate)
    if(timeStep==12.0d){
      DateFormatUtil.YYYYMMDDHHMMStr(fcdate)
    }else if(timeStep==1.0d){
      DateFormatUtil.YYYYMMDDHHMMStr(dateOp.plusHour(fcdate, timeStep.toInt))
    }else{
      val e:Exception=new Exception("入库前timeStep="+timeStep+",没有配置相应参数")
      e.printStackTrace()
      null
    }
  }

  def ObsGetFc (obsdate: Date): Date = {
    val Fcdate = DateFormatUtil.YYYYMMDDHH0(this.ObsGetFcDateStr(DateFormatUtil.YYYYMMDDHHStr0(obsdate)))
    Fcdate
  }

  //符合新入库时间的规则YYYYMMDDHH为changgeReftime时间,后两位为入库时次即实况小时
  def ObsGetKVDateStr_New(obsdate:Date,timeStep:Double): String={
    val fcdate=this.ObsGetFc(obsdate)
    val HHStr: String = DateFormatUtil.HHStr(obsdate)

    val KVDateStr_New:String={
      if (timeStep == 12.0d || timeStep == 24.0d) {
        DateFormatUtil.YYYYMMDDHHStr0(fcdate) + HHStr
      }else if(timeStep==1.0d){
        DateFormatUtil.YYYYMMDDHHStr0(dateOp.plusHour(fcdate, timeStep.toInt)) + HHStr
      }else{
        val e: Exception = new Exception("ObsGetKVDateStr_New入库前timeStep=" + timeStep + ",没有配置相应参数")
        e.printStackTrace()
        throw e
        null
      }
    }

    KVDateStr_New
  }
  def KVDateStrGetObs_New(KVDateStr_New:String,timeStep:Double): Date ={
    val kvStr=KVDateStr_New.substring(0,10)
    val hhStr:String=KVDateStr_New.substring(10,12)
    val kvdate: Date = DateFormatUtil.YYYYMMDDHH0(kvStr)
    val fcdate:Date={
      var fcdate0:Date={
        if (timeStep == 12.0d || timeStep == 24.0d) {
          kvdate
        }else if(timeStep==1.0d){
          dateOp.plusHour(kvdate,-timeStep.toInt)
        }else{
          val e:Exception=new Exception("KVDateStrGetObs,timeStep="+timeStep+",没有配置相应参数")
          e.printStackTrace()
          null
        }
      }
      val hhStrs0:Array[String]=Array("00","01","02","03","04","05","06","07","08")
      if(hhStrs0.contains(hhStr)){
        fcdate0=dateOp.plusDay(fcdate0,1)
      }

      fcdate0
    }
    val HH: Int = DateFormatUtil.HH(fcdate)

    val obsdateStr:String={
      if(HH==8){
        DateFormatUtil.YYYYMMDDStr0(fcdate) + hhStr
      }else if(HH ==20){
        DateFormatUtil.YYYYMMDDStr0(fcdate) + hhStr
      }else{
        val e:Exception=new Exception("KVDateStrGetObs,HH="+HH+",没有配置相应参数")
        e.printStackTrace()
        null
      }
    }
    val obsdate: Date = DateFormatUtil.YYYYMMDDHH0(obsdateStr)
    obsdate
  }
  /*def KVDateStrGetObs(KVDateStr_New:String,timeStep:Double): Date ={

    val HH:Int=DataFormatUtil.HH(fcdate)
//    val HHStr:String=DataFormatUtil.HHStr(fcdate)
    val obsdateStr:String={
      if(HH==8){
        DataFormatUtil.YYYYMMDDStr0(fcdate)+hhStr
      }else if(HH ==20){
        DataFormatUtil.YYYYMMDDStr0(fcdate)+HHStr
      }else{
        val e:Exception=new Exception("KVDateStrGetObs,timeStep="+timeStep+",没有配置相应参数")
        e.printStackTrace()
        null
      }
    }
    val obsdate:Date=DataFormatUtil.YYYYMMDDHH0(obsdateStr)
    obsdate
  }*/

  def ObsGetOcf(obsdate:Date): Date ={
    val Ocfdate = DateFormatUtil.YYYYMMDDHH0(this.ObsGetOcfDateStr(DateFormatUtil.YYYYMMDDHHStr0(obsdate)))
    Ocfdate
  }

  def KVGetFc(kvdate:Date): Date ={
    val Fcdate = DateFormatUtil.YYYYMMDDHH0(this.KVGetFcDateStr(DateFormatUtil.YYYYMMDDHHStr0(kvdate)))
    Fcdate
  }

  def OcfGetStartDate (ocfdate: Date): Date = {
    val startDate = DateFormatUtil.YYYYMMDDHH0(this.OcfGetStartDateStr(DateFormatUtil.YYYYMMDDHHStr0(ocfdate)))
    startDate
  }

  def FcGetOcf(fcdate:Date): mutable.TreeSet[Date] ={
    val OcfDateSet=new mutable.TreeSet[Date]()
    val h:Int=fcdate.getHours
    if(h==8){
      OcfDateSet.add(sdfYMDH.parse(sdfYMD06.format(fcdate)))
      OcfDateSet.add(sdfYMDH.parse(sdfYMD08.format(fcdate)))
      OcfDateSet.add(sdfYMDH.parse(sdfYMD12.format(fcdate)))
    }else{
      OcfDateSet.add(sdfYMDH.parse(sdfYMD20.format(fcdate)))
    }
    OcfDateSet
  }
  def FcGetObs(fcdate:Date): mutable.TreeSet[Date] ={
    val ObsDateSet=new mutable.TreeSet[Date]()
    for(i<- 1 to 12){
      val thedate=dateOp.plusHour(fcdate,i)
      ObsDateSet.add(thedate)
    }
    ObsDateSet
  }

  def FcGetStartDate(fcdate:Date,timeStep:Double): Date ={
    DateOperation.plusHour(fcdate, timeStep.toInt)
  }

  /*private def OcfGetFc0(ocfdate:Date): Date ={
    val Fcdate=DataFormatUtil.YYYYMMDDHH0(this.OcfGetFcDateStr0(DataFormatUtil.YYYYMMDDHHStr0(ocfdate)))
    Fcdate
  }*/
  private def OcfGetFc (ocfdate: Date): Date = {
    val Fcdate = DateFormatUtil.YYYYMMDDHH0(this.OcfGetFcDateStr(DateFormatUtil.YYYYMMDDHHStr0(ocfdate)))
    Fcdate
  }

  def OcftimeStampGetFc (timeStamp: Date): Date = {
    this.ObsGetFc(timeStamp)
  }

  /*private def OcfGetFcDateStr0(OcfdateStr: String): String = {
    val ocfdate = sdfYMDH.parse(OcfdateStr)
    val hours = ocfdate.getHours
    var FcDateStr = ""
    if (hours == 6 || hours == 8 || hours == 12) FcDateStr = sdfYMD08.format(ocfdate)
    else if (hours == 20) FcDateStr = sdfYMD20.format(ocfdate)
    FcDateStr
  }*/

  private def OcfGetStartDateStr(OcfdateStr:String):String={
    val ocfdate = sdfYMDH.parse(OcfdateStr)
    val hours = ocfdate.getHours
    val StartDateStr = {
      if(hours == 6 || hours == 8 || hours == 12){
        sdfYMD08.format(ocfdate)
      }else if (hours == 20){
        sdfYMD20.format(ocfdate)
      }else{
        ""
      }
    }

    StartDateStr
  }

  private def OcfGetFcDateStr(OcfdateStr: String): String ={
    val ocfdate = sdfYMDH.parse(OcfdateStr)
    val hours = ocfdate.getHours
    val FcDateStr = {
      if (hours == 6 ) {
        val ocfdate0 = DateOperation.plusDay(ocfdate, -1)
        sdfYMD20.format(ocfdate0)
      }else if(hours == 8 || hours == 12){
        sdfYMD08.format(ocfdate)
      }else if (hours == 20){
        sdfYMD20.format(ocfdate)
      }else{
        ""
      }
    }

    FcDateStr
  }
  private def ObsGetFcDateStr(ObsdateStr: String): String = {
    var obsdate = sdfYMDH.parse(ObsdateStr)
    val hours = obsdate.getHours
    val FcDateStr :String={
      if (hours <= 8) {
        obsdate = dateOp.plusDay(obsdate, -1)
        sdfYMD20.format(obsdate)
      }else if (8 < hours && hours <= 20){
        sdfYMD08.format(obsdate)
      }else{
        sdfYMD20.format(obsdate)
      }
    }
    FcDateStr
  }
  private def ObsGetOcfDateStr(ObsdateStr: String): String ={
    var obsdate = sdfYMDH.parse(ObsdateStr)
    val hours = obsdate.getHours
    val OcfDateStr :String={
      if(hours<6){
        obsdate = dateOp.plusDay(obsdate, -1)
        sdfYMD20.format(obsdate)
      }else if (6<=hours && hours < 8) {
        sdfYMD06.format(obsdate)
      }else if (8 <= hours && hours < 12) {
        sdfYMD08.format(obsdate)
      }else if(12 <= hours && hours < 18){
        sdfYMD12.format(obsdate)
      }else{
        sdfYMD20.format(obsdate)
      }
    }
    OcfDateStr
  }

  //added by lijing on 2018/5/24
  private def KVGetFcDateStr(KVdateStr: String): String = {
    var obsdate = sdfYMDH.parse(KVdateStr)
    val hours = obsdate.getHours
    val FcDateStr :String={
      if (hours < 6) {
        obsdate = dateOp.plusDay(obsdate, -1)
        sdfYMD20.format(obsdate)
      }else if (6 <= hours && hours < 18){
        sdfYMD08.format(obsdate)
      }else{
        sdfYMD20.format(obsdate)
      }
    }
    FcDateStr
  }

  //modified by lijing 2018/05/25
  def previousFCDate(date: Date): Date = {
    val hours = date.getHours
    val out_dateStr = {
      if (hours > 20) {
        sdfYMD20.format(date)
      }else if (hours <= 8) {
        sdfYMD20.format(dateOp.plusDay(date, -1))
      }else{
        sdfYMD08.format(date)
      }
    }
    val out_date = sdfYMDH.parse(out_dateStr)
    out_date
  }
  def nextFCDate(date: Date): Date = {
    val hours = date.getHours
    val out_dateStr:String={
      if (hours <= 8){
        sdfYMD08.format(date)
      }else if (hours > 20) {
        sdfYMD08.format(dateOp.plusDay(date, 1))
      }else{
        sdfYMD20.format(date)
      }
    }
    val out_date = sdfYMDH.parse(out_dateStr)
    out_date
  }

  //added by wufenqiang 2018/7/9
  def previousObsDate(date: Date): Date ={
    val mod0:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH0000")
    val mod1:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmSS")
    val thedateStr:String=mod0.format(date)
    mod1.parse(thedateStr)
  }
  def nextObsDate(date: Date): Date ={
    val previousObs=this.previousObsDate(date)
    dateOp.plusHour(previousObs,1)
  }

  //added by wufenqiang 2018/8/29
  def is08(fcdate:Date): Boolean ={
    val hour:Int=fcdate.getHours
    if(hour==8) {
      true
    }else if(hour==20){
      false
    }else{
      val msg="hour="+hour+"不是起报时间"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
      //      println(msg)
      false
    }
  }
  def is20(fcdate:Date): Boolean =(!this.is08(fcdate))

}
object WeatherDate{
  private val wd=new WeatherDate()
  def is08(fcdate:Date)=wd.is08(fcdate)
  def is20(fcdate:Date)=wd.is20(fcdate)
  def ObsGetFc(obsdate:Date): Date =wd.ObsGetFc(obsdate)
  def OcfGetFc(ocfdate:Date): Date =wd.OcfGetFc(ocfdate)

  def OcftimeStampGetFc (timeStamp: Date): Date = wd.OcftimeStampGetFc(timeStamp)
  def ObsGetOcf(obsdate:Date): Date =wd.ObsGetOcf(obsdate)

  def OcfGetStartDate(ocfdate:Date): Date =wd.OcfGetStartDate(ocfdate)
  def FcGetStartDate(fcdate:Date,timeStep:Double):Date=wd.FcGetStartDate(fcdate,timeStep)

  def FcGetObs(fcdate:Date):mutable.TreeSet[Date]=wd.FcGetObs(fcdate)
  def FcGetOcf(fcdate:Date):mutable.TreeSet[Date]=wd.FcGetOcf(fcdate)

  def previousFCDate(date: Date): Date=wd.previousFCDate(date)
  def ObsGetKVDateStr_Old(obsdate:Date,timeStep:Double):String=wd.ObsGetKVDateStr_Old(obsdate,timeStep)
  def ObsGetKVDateStr_New(obsdate:Date,timeStep:Double): String=wd.ObsGetKVDateStr_New(obsdate,timeStep)
  def KVDateStrGetObs_New(KVDateStr_New:String,timeStep:Double):Date=wd.KVDateStrGetObs_New(KVDateStr_New,timeStep)

  def previousObsDate(date: Date): Date=wd.previousObsDate(date)

  def nextFCDate (date: Date): Date = wd.nextFCDate(date)
  def main(args:Array[String]): Unit ={
    /*val timeSteps: Array[Double] = Array(1.0d, 12.0d)
    val obsdateStrs: Array[String] = Array("20190228000000", "20190228010000", "20190228020000", "20190228030000", "20190228040000", "20190228050000", "20190228060000", "20190228070000", "20190228080000", "20190228090000", "20190228100000", "20190228110000", "20190228120000",
      "20190228130000", "20190228140000", "20190228150000", "20190228160000", "20190228170000", "20190228180000", "20190228190000", "20190228200000", "20190228210000", "20190228220000", "20190228230000")
    timeSteps.foreach(timeStep => {
      obsdateStrs.foreach(obsdateStr => {
        val obsdate = DateFormatUtil.YYYYMMDDHHMMSS0(obsdateStr)
        val reftime = this.ObsGetKVDateStr_New(obsdate, timeStep)
        println("obsdate=" + obsdateStr + ";timeStep=" + timeStep + ";reftime=" + reftime)
      })
    })*/
    val ocfdateStrs = Array("20190318060000", "20190318080000", "20190318120000", "20190318200000")
    ocfdateStrs.foreach(ocfdateStr => {
      val ocfdate = DateFormatUtil.YYYYMMDDHHMMSS0(ocfdateStr)
      val ocfstart = this.OcfGetStartDate(ocfdate)
      val ocfstartStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfstart)
      println(ocfdateStr + "=>" + ocfstartStr)
    })

  }
}
