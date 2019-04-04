package com.weather.bigdata.mt.basic.mt_commons.business.InputKVStore

import java.util.{Calendar, Date}

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.operation.{ArrayOperation, DateOperation, StrOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Interception.DataInterceptionCW
import com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil.{KVStoreInterpolationCW, RddKVStoreCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object InputKVStoreAllCW2 {

  //  private val AO = new ArrayOperation
  private val StrOp = StrOperation
  //入库数据输出抽查站开关
  private val checkStationOpen: Boolean = PropertiesUtil.checkStationOpen
  //  private val TwelveH_Hours = 240.0d
  private val OneH_Hours=72.0d
  private val dateOp: DateOperation = new DateOperation

  private def getinputKVScidata_12_08(scidata0:SciDatasetCW,fcdate:Date): SciDatasetCW = {
    KVStoreInterpolationCW.twelve2twenty(scidata0)
  }
  private def getinputKVScidata_12_20(scidata20:SciDatasetCW,fcdate:Date): SciDatasetCW = {
    //    val jsonRdd=jsonRdds._1
    val TwelveH_Hours = 240.0d
    val timeStep=12.0d
    val times = ArrayOperation.ArithmeticArray(timeStep, TwelveH_Hours, timeStep)
    //拼接的时间
    val cal = Calendar.getInstance
    cal.setTime(fcdate)
    cal.add(Calendar.HOUR_OF_DAY, -12)
    val FcLastdate = cal.getTime( )
    val times08 = ArrayOperation.ArithmeticArray(timeStep, timeStep, timeStep)

    val fcType=AttributesOperationCW.getFcType(scidata20)
    val idName=AttributesOperationCW.getIDName(scidata20)

    val scidata12: SciDatasetCW ={
      val dataNames = PropertiesUtil.getwElement(fcType, timeStep)
      //20时数据裁剪
      val fcScidata20 = DataInterceptionCW.datacutByTime(scidata20, dataNames, times)
      //08时数据读取
      val Scidata08 = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, FcLastdate)
      //08时数据裁剪
      val fcScidata08 = DataInterceptionCW.datacutByTime(Scidata08, dataNames, times08)
//      Scidata08=null
      //20和08时数据合并
      val IterableScidatas: Iterable[SciDatasetCW] = Iterable(fcScidata20,fcScidata08)
//      fcScidata20=null
//      fcScidata08=null

      //数据裁剪
      val addTime=12
      val fcRdd0 = DataInterceptionCW.dataunionByTime(fcdate,addTime, IterableScidatas, times, dataNames)

      fcRdd0
    }

    val scidata24 = KVStoreInterpolationCW.twelve2twenty(scidata12)
    scidata24
  }
  /*private def getinputKVData_1_cut(jsonRdd: RDD[JSONObject],date:Date,timeStep:Double,fcTypes:Array[String]): Array[RDD[SciDataset]]  ={

    val fcdate=WeatherDate.previousFCDate(date)
    val nowHOUR= date.getHours

    val startTime=nowHOUR match{
      case hh if(hh>=9&hh<21)=> hh-8
      case hh if(hh>=21)=> hh-20
      case hh if(hh<9) =>4+nowHOUR
    }


    val times=AO.ArithmeticArray(startTime.toDouble,this.OneH_Hours.toInt,timeStep)
    //    val jsonRdd=jsonRdds._1
    val fcRdd=fcTypes.map(
      fcType=>{
        //        val FcNcFile=PropertiesUtil.getfcPath(timeStep,fcType,fcdate)
        /*val rdd = ReadFile.ReadNcRdd(FcNcFile)*/
        val rdd =ReadFile.ReadFcRdd(jsonRdd,timeStep,fcType,fcdate)
        val dataNames=PropertiesUtil.getwElement(fcType,timeStep)
        //数据裁剪
        val fcRdd0=DataInterception.datacutByTime(rdd,dataNames,times)
        fcRdd0
      })

    fcRdd
  }*/

  private def getinputKVScidata_1(scidata0:SciDatasetCW,fcdate:Date): SciDatasetCW  ={
    scidata0
  }

  private def getinputKVScidata_12(scidata0:SciDatasetCW,fcdate:Date): SciDatasetCW ={
    if(fcdate.getHours==20){
      this.getinputKVScidata_12_20(scidata0,fcdate)
    }else{
      this.getinputKVScidata_12_08(scidata0,fcdate)
    }
  }


  private def getinputKVDate(splitFile: String, date: Date, timeSteps: Array[Double], fcTypes: Array[String], idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): RDD[(String,Iterable[((String,Double),Iterable[String])])]  = {
    val fcdate=WeatherDate.ObsGetFc(date)
    ReadNcCW.ReadKVRddArrConf(splitFile, timeSteps, fcTypes, fcdate, idNameMaps)
  }

  /*private def putKVData_Return(obsdate: Date, fcRdd: Array[RDD[SciDataset]],stationsInfos: (mutable.HashMap[String,String],String,Date)): Boolean = {
    //    val fcdate=WeatherDate.ObsGetFc(obsdate)

    /*try{
      RddKVStore.rdd2Array3(fcdate, fcRdd, stationsInfos)
      true
    }catch{
      case e:Exception=>{
        val e0:Exception=new Exception("rdd2Array3_2入库异常"+e)
        e0.printStackTrace()
      }
        false
    }*/

    RddKVStore.rdd2Array3_Return(obsdate, fcRdd, stationsInfos)
  }*/

  private def putKVData_Action(obsdate: Date, fcRdds: RDD[(String,Iterable[((String,Double),Iterable[String])])],stationsInfos: (mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]],appID:String): Unit ={
    val fcdate=WeatherDate.ObsGetFc(obsdate)
    fcRdds.foreach(f=>{
      val groupKey=f._1
      val ite0:Iterable[((String,Double),Iterable[String])]=f._2
      println("groupKey="+groupKey+"开始,groupSize="+ite0.size)
      ite0.foreach(it=>{
        val idName=it._1._1
        val timeStep=it._1._2
        val arrScidata:Iterable[SciDatasetCW]=it._2.map(
          fcType=>{
            val scidata = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdate)
            if(timeStep==1.0d){
              this.getinputKVScidata_1(scidata,fcdate)
            }else if(timeStep==12.0d){
              this.getinputKVScidata_12(scidata,fcdate)
            }else{
              val e:Exception=new Exception("没有配置timeStep="+timeStep+"入库")
              e.printStackTrace()
              null
            }
          }
        )
        RddKVStoreCW.ArrScidata2Array(obsdate, ((idName,timeStep),arrScidata), stationsInfos,appID,idNameMaps)
      })
    })
  }
  private def putKVData_ReturnFlag(obsdate: Date, fcRdds: RDD[(String,Iterable[((String,Double),Iterable[String])])],stationsInfos: (mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]],appID:String): Boolean ={
    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val flag:Boolean=fcRdds.map(f=>{
      val groupKey=f._1
      val ite0:Iterable[((String,Double),Iterable[String])]=f._2
      println("groupKey="+groupKey+"开始,groupSize="+ite0.size)
      val flag0:Boolean=ite0.map(it=>{
        val idName=it._1._1
        val timeStep=it._1._2
        val arrScidata:Iterable[SciDatasetCW]=it._2.map(
          fcType=>{
            val scidata = ReadNcCW.ReadFcScidata(idName, timeStep, fcType, fcdate)
            if(timeStep==1.0d){
              this.getinputKVScidata_1(scidata,fcdate)
            }else if(timeStep==12.0d){
              this.getinputKVScidata_12(scidata,fcdate)
            }else{
              val e:Exception=new Exception("没有配置timeStep="+timeStep+"入库")
              e.printStackTrace()
              null
            }
          }
        )
        RddKVStoreCW.ArrScidata2Array(obsdate, ((idName,timeStep),arrScidata), stationsInfos,appID,idNameMaps)
      }).reduce((x,y)=>(x && y))
      flag0
    }).reduce((x,y)=>(x && y))
    flag
  }

  private def parallelinputKVStore(splitFile:String, date: Date, timeSteps: Array[Double], fcTypes:Array[String], stationsInfos: (mutable.HashMap[String,String],String,Date), idNameMaps:mutable.HashMap[String,Array[(String,Double)]], appID:String): Boolean = {
    val ArrRddsStart = System.currentTimeMillis

    //1h\12h分别入库
    /*timeSteps.foreach(timeStep=>{
      val ArrRdds: Array[RDD[SciDataset]] = {
        val arr = getinputKVDate(jsonRdd,date, timeStep,fcTypes)
        val ArrRddsEnd = System.currentTimeMillis( )
        val msg0=ws.showDateStr("getinputKVData:", ArrRddsStart, ArrRddsEnd)
        println(msg0)
        arr
      }
      val putStart = System.currentTimeMillis( )
      this.putKVData_Return(date,ArrRdds,stationsInfos)
      val putEnd = System.currentTimeMillis( )
      val msg1=ws.showDateStr("putKVData_Return:", putStart, putEnd)
      println(msg1)
      val msg2=ws.showDateStr("inputKVStore,所有过程:", ArrRddsStart, putEnd)
      println(msg2)
    })*/

    //1h\12h并行入库
    val ArrRdds: RDD[(String,Iterable[((String,Double),Iterable[String])])] = {
      val arr = getinputKVDate(splitFile,date, timeSteps,fcTypes,idNameMaps)
      val ArrRddsEnd = System.currentTimeMillis( )
      WeatherShowtime.showDateStrOut1("getinputKVData:", ArrRddsStart, ArrRddsEnd)
      arr
    }
    val putStart = System.currentTimeMillis( )
    val flag=this.putKVData_ReturnFlag(date,ArrRdds,stationsInfos,idNameMaps,appID)
    val putEnd = System.currentTimeMillis( )


    WeatherShowtime.showDateStrOut1("putKVData:", putStart, putEnd)
    WeatherShowtime.showDateStrOut1("inputKVStore,所有过程:", ArrRddsStart, putEnd)
    flag
  }

  def maininputKV(jsonFile: String,date: Date, timeSteps: Array[Double],fcTypes:Array[String],stationsInfos: (mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): Boolean ={
    val appID=ContextUtil.getApplicationID
    this.parallelinputKVStore(jsonFile,date, timeSteps,fcTypes,stationsInfos,idNameMaps,appID)
  }

  /*  def main(args: Array[String]): Unit = {
      val dateStr = args(0)
      val date = DataFormatUtil.YYYYMMDDHHMMSS0(dateStr)
      val timeSteps=ConstantUtil.TimeSteps
      val fcTypes=ConstantUtil.FcTypes

      val jsonFile=PropertiesUtil.getjsonFile()
      val jsonRdd=PropertiesUtil.getJsonRdd(jsonFile)
      val appID=ContextUtil.getApplicationID
      val jsonArr=PropertiesUtil.getJsonArr(jsonFile)
      val idNameMaps:mutable.HashMap[String,Array[(String,Double)]]=MatchSciDatas.getidNameMap(timeSteps,jsonArr)


      val stationsInfo: mutable.HashMap[String,String] = {
        if (checkStationOpen) {
          println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
          ReadFile.ReadStationInfoMap_IdGetlatlon0()
        } else {
          println("输出入库前数据站点抽查(pom.checkStation.Open)关闭")
          null
        }
      }
      val stationinputKV=PropertiesUtil.stationinputKV
      val stationsInfos=(stationsInfo,stationinputKV,date)
      if (args.length == 1) {
        this.parallelinputKVStore(jsonRdd,date,timeSteps,fcTypes,stationsInfos,idNameMaps,appID)
      } /*else {
        val timeStep = args(1).toDouble
        if (args.length > 2) {
          val fcTypes = StrOp.remove(StrOp.remove(args, 0), 0)
          val inputflag=this.parallelinputKVStore(obsdate)
        } else {
          if (timeStep == 1.0d) {
            this.inputKVStore_1(obsdate, timeStep)
          } else if (timeStep == 12.0d) {
            this.inputKVStore_12(obsdate, timeStep)
          } else {
            val e: Exception = new Exception("timeStep=" + timeStep + "不入库")
            e.printStackTrace( )
          }
        }
      }*/
    }*/
}
