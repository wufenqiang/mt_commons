package com.weather.bigdata.mt.basic.mt_commons.business.Fc

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.ElementUtil.{ElementInterpolationCW, WeatherElementCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil.{LatLonInterpolationCW, TimeInterpolationCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.ParallelUtil.ParallelProCW
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import com.weather.bigdata.mt.basic.mt_commons.commons.QualityControlUtil.{ForwardControlCW, FrontControlCW, PostControlCW}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.AccordSciDatas_JsonObjectCW
import com.weather.bigdata.mt.basic.mt_commons.commons.UnitUtil.NMCoffsetCW
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object FcMainCW1 {
  private val fcPostOpen = PropertiesUtil.fcPostOpen
  private val checkStationOpen = PropertiesUtil.checkStationOpen
  private val gr2bFrontOpen = PropertiesUtil.gr2bFrontOpen
  private val gr2bForwardOpen = PropertiesUtil.gr2bForwardOpen
  //  private val fcParallel: Boolean = PropertiesUtil.fcParallel


  def nmc2fcRdd(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata: SciDatasetCW, ExtremMap0: Map[String, Map[String, Array[Float]]], gr2bFrontOpen: Boolean, timeStep: Double, appID: String): RDD[SciDatasetCW] = {
    this.nmc2fcRdd(jsonRdd, fcType, fcdate, unSplitScidata, ExtremMap0, gr2bFrontOpen, Array(timeStep), appID)
  }

  def parallelfc_ReturnRDD(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata: SciDatasetCW, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): RDD[SciDatasetCW] = {
    val appID = ContextUtil.getApplicationID
    /*val parallelfcRdd:RDD[SciDataset]=this.getparallelfcRdd(jsonRdd,fcType,timeSteps,fcdate,gr2bfileName,appID)
    val outRdd:RDD[SciDataset]=parallelfcRdd.map(
      scidata0=>{
        val scidata=this.parallelBeforeOut(scidata0,ExtremMap0,stationsInfos,appID)
        //输出nc
        WriteFile.WriteFcFile_addId_tmp(scidata,appID)
        scidata
      })*/

    //获取有nmc直接到fc生产的rdd且抽取站点
    var rdd = this.nmc2fcRddPlusStation(jsonRdd, fcType, fcdate, unSplitScidata, ExtremMap0, stationsInfos, gr2bFrontOpen: Boolean, timeSteps)

    //输出nc
    rdd = rdd.map(scidata => {
      WriteNcCW.WriteFcFile_addId_tmp(scidata, appID)
      scidata
    })

    rdd
  }

  def parallelfc_ReturnFlag(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata: SciDatasetCW, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): Boolean = {
    val appID = ContextUtil.getApplicationID

    //获取有nmc直接到fc生产的rdd且抽取站点
    val rdd = this.nmc2fcRddPlusStation(jsonRdd, fcType, fcdate, unSplitScidata: SciDatasetCW, ExtremMap0, stationsInfos, gr2bFrontOpen, timeSteps)

    //输出nc成功标志的与逻辑
    val flag: Boolean = rdd.map(scidata => {
      WriteNcCW.WriteFcFile_addId_tmp(scidata, appID)
    }).reduce((x, y) => (x && y))

    flag
  }

  private def nmc2fcRddPlusStation(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata: SciDatasetCW, ExtremMap0: Map[String, Map[String, Array[Float]]], stationsInfos: (mutable.HashMap[String, String], String, Date), gr2bFrontOpen: Boolean, timeSteps: Array[Double]): RDD[SciDatasetCW] = {
    val appID = ContextUtil.getApplicationID

    //获取有nmc直接到fc生产的rdd
    var rdd = this.nmc2fcRdd(jsonRdd, fcType, fcdate, unSplitScidata, ExtremMap0, gr2bFrontOpen, timeSteps, appID)

    //抽取站点
    rdd = rdd.map(scidata => {
      if (checkStationOpen) {
        val stationsInfo: mutable.HashMap[String, String] = stationsInfos._1
        val stationfc = stationsInfos._2
        val fcdate = stationsInfos._3
        //        val checkOutRootPath=PropertiesUtil.checkoutRootPath
        //        val stationfc=PropertiesUtil.stationfc
        CheckStationCW.checkStation(appID, stationsInfo, stationfc, scidata, fcdate, fcdate)
      } else {
        println("CheckStation开关关闭:" + checkStationOpen)
      }

      scidata
    })

    rdd
  }

  def nmc2fcRdd(jsonRdd: RDD[JSONObject], fcType: String, fcdate: Date, unSplitScidata: SciDatasetCW, ExtremMap0: Map[String, Map[String, Array[Float]]], gr2bFrontOpen: Boolean, timeSteps: Array[Double], appID: String): RDD[SciDatasetCW] = {

    //读取并获取拆分后的RDD
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()
    val varsName = (timeName, heightName, latName, lonName)
    //    val fcDatasets_unSplit:SciDataset=ReadFile.ReadGr2bFile(gr2bfileName,fcdate,fcType)

    //数据拆分
    var rdd: RDD[SciDatasetCW] = AccordSciDatas_JsonObjectCW.AccSciRdd_reName(jsonRdd, unSplitScidata, timeName, heightName, latName, lonName, fcType, fcdate)

    //前置质控
    rdd = {
      if (gr2bFrontOpen) {
        val front: Boolean = FrontControlCW.frontControl(rdd)
        if (!front) {
          println("本次更新数据为" + fcType + "前置质控数据出现异常,不进行余下计算")
          return null
        } else {
          println("前置质控数据正常,开始余下计算")
          rdd
        }
      } else {
        println("前置质控关闭,不进行质控直接进行计算,gr2bFrontOpen=" + gr2bFrontOpen)
        rdd
      }
    }

    //修复gr2b数据
    rdd = {
      if (gr2bForwardOpen) {
        ForwardControlCW.forwardControl(rdd)
      } else {
        println("gr2b数据源lat=0数据修复关闭.gr2bForwardOpen=" + gr2bForwardOpen)
        rdd
      }
    }

    //单位转换(TEM)
    rdd = {
      if (Constant.TEM.equals(fcType) || Constant.PRS.equals(fcType)) {
        //          val unitStart=System.currentTimeMillis()
        val rdd0 = NMCoffsetCW.dataoffsetNMC(rdd)
        //          val unitEnd=System.currentTimeMillis()
        //          ws.showDateSystemOut("("+fcType+")unitChange:",unitStart,unitEnd)
        rdd0
      } else {
        rdd
      }
    }


    //空间插值
    rdd = {
      val rdataNames_rename = PropertiesUtil.getrElement_rename(fcType)
      val beginStep: Double = 0.05d
      val endStep: Double = 0.01d
      val rdd0 = LatLonInterpolationCW.Interpolation(rdd, beginStep, endStep, timeName, heightName, latName, lonName, rdataNames_rename)
      rdd0
    }

    //并行预处理
    rdd = {
      //val rdd0=CpRdd.cprddbytimeStep(jsonRdds,rdd,timeSteps,fcType)
      val rdd0 = ParallelProCW.cprddbytimeStep(rdd, timeSteps, fcType)
      rdd0
    }

    //并行内处理
    rdd = rdd.map(scidata0 => {
      val fcType = AttributesOperationCW.getFcType(scidata0)
      val timeStep = AttributesOperationCW.gettimeStep(scidata0)
      val idName = AttributesOperationCW.getIDName(scidata0)


      //      //12h UV风速输出
      //      if(Constant.WIND.equals(fcType) && timeStep == 12.0d){
      //        val times=PropertiesUtil.gettimes(timeStep)
      //        val dataNames_timeIA:Array[(String,String)]=PropertiesUtil.getdataNames_alTypetimeIAs_from3h(fcType,timeStep)
      //        val timeSci = TimeInterpolation.timeIA(scidata0, varsName, dataNames_timeIA, times)
      //
      //        val oldName:String=timeSci.datasetName
      //        val newName=oldName.replace(Constant.WIND,Constant.WINUV)
      //        timeSci.setName(newName)
      //        AttributesOperation.updateFcType(timeSci,Constant.WINUV)
      //        val flag=WriteNc.WriteFcFile_addId_tmp(timeSci,appID)
      //        if(flag){
      //          val msg=Constant.WINUV+",fcdate="+",timeStep="+timeStep+"生产成功"
      //          println(msg)
      //        }
      //        timeSci.setName(oldName)
      //        AttributesOperation.updateFcType(timeSci,Constant.WIND)
      //      }


      //12h风速风向转换(WIND)12h:UV2SD
      var scidata: SciDatasetCW = {
        if (Constant.WIND.equals(fcType) && timeStep == 12.0d) {
          WeatherElementCW.WindUV2SD(scidata0)
        } else {
          scidata0
        }
      }


      //时间插值
      scidata = {
        if (Constant.WIND.equals(fcType) && timeStep == 12.0d) {
          ElementInterpolationCW.eleInterpolation(scidata)
        } else {
          val times = PropertiesUtil.gettimes(timeStep)
          val dataNames_timeIA: Array[(String, String)] = PropertiesUtil.getdataNames_alTypetimeIAs_from3h(fcType, timeStep)
          val timeSci = TimeInterpolationCW.timeIA(scidata, varsName, dataNames_timeIA, times)
          timeSci
        }
      }

      //后置质控
      scidata = {
        if (fcPostOpen && fcType.equals(Constant.TEM)) {
          /*val ExtremMap={
            if(ExtremMap0 != null){
              ExtremMap0
            }else{
              val TemExtremPath=PropertiesUtil.getTemExtremPath()
              PostControl.getExtremMap(fcType,TemExtremPath)
            }
          }*/
          val postSci: SciDatasetCW = PostControlCW.postControl(scidata, ExtremMap0)
          postSci
        } else {
          println("后置质控关闭,fcPostOpen=" + fcPostOpen)
          scidata
        }
      }


      //1h UV风速输出
      scidata = {
        if (Constant.WIND.equals(fcType) && timeStep == 1.0d) {
          val oldName: String = scidata.datasetName
          val newName = oldName.replace(Constant.WIND, Constant.WINUV)
          scidata.setName(newName)
          AttributesOperationCW.updateFcType(scidata, Constant.WINUV)
          val flag = WriteNcCW.WriteFcFile_addId_tmp(scidata, appID)
          if (flag) {
            val msg = Constant.WINUV + ",fcdate=" + ",timeStep=" + timeStep + "生产成功"
            println(msg)
          }
          scidata.setName(oldName)
          AttributesOperationCW.updateFcType(scidata, Constant.WIND)
        }
        scidata
      }


      //风速风向转换(WIND)1h:UV2SDG,12h:SD2SDG
      scidata = {
        if (Constant.WIND.equals(fcType) && timeStep == 1.0d) {
          WeatherElementCW.WindUV2SDG(scidata)
        } else if (Constant.WIND.equals(fcType) && timeStep == 12.0d) {
          WeatherElementCW.WindSD2SDG(scidata)
        } else {
          scidata
        }
      }

      scidata

    })

    //输出rdd
    rdd
  }

  /* private def mainfc(jsonsRdd:RDD[JSONObject], fcType:String, date:Date, unSplitScidata:SciDataset, ExtremMap0:Map[String,Map[String,Array[Double]]], stationsInfos: (mutable.HashMap[String,String],String,Date), timeSteps:Array[Double], parallel:Boolean=this.fcParallel, gr2bFrontOpen:Boolean=this.gr2bFrontOpen): Boolean ={
     val flag=if(parallel){
       this.parallelfc_ReturnFlag(jsonsRdd,fcType, date, unSplitScidata,ExtremMap0,stationsInfos,gr2bFrontOpen,timeSteps)
     }else{
       timeSteps.map(timeStep=>this.fc(jsonsRdd,fcType, date, unSplitScidata,ExtremMap0,stationsInfos,timeStep,gr2bFrontOpen)).reduce((x,y)=>(x && y))
     }
     flag
   }*/

}
