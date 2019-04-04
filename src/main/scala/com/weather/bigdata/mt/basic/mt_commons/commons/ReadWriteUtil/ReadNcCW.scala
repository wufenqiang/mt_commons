package com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesOperationCW, ReadSciCW}
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1}
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil.IdNameMap
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.{ContextUtil, workersUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ReadNcCW {

  private def read(fileName: String, vars: List[String]): SciDatasetCW = {
    val scidata = ReadSciCW.read(fileName, vars)
    AttributesOperationCW.setReftimeOpenDefault(scidata)
    scidata
  }

  def ReadGeoFile (): SciDatasetCW = {
    val geoFile = PropertiesUtil.getGeoFile( )
    /*val sciGeo = ReadFile.ReadNcScidata(geoFile)
    sciGeo*/
    this.ReadGeoFile(geoFile)
  }

  def ReadGeoFile (geoFile: String): SciDatasetCW = {
    val Start = System.currentTimeMillis( )
    val sciGeo = ReadNcCW.ReadNcScidata(geoFile)
    val End = System.currentTimeMillis( )

    val msg0 = "geoFile=" + geoFile
    WeatherShowtime.showDateStrOut1("ReadGeoFile:", Start, End, msg0)
    sciGeo
  }

  def ReadNcScidata (fileName: String): SciDatasetCW = {
    this.read(fileName, Nil)
  }

  def ReadGr2bFile (gr2bfileName: String, fcdate: Date, fcType: String): SciDatasetCW = {
    val appId = ContextUtil.getApplicationID

    val scidata: SciDatasetCW = this.ReadGr2b(gr2bfileName)

    //-----------------gr2b数据采集日志接入--------------
    //    val changePerson: String = "罗冰"
    //    //gr2b数据上游采集人员
    //    var basicRemark: JSONObject = new JSONObject( )
    //    basicRemark = AttributesOperationCW.addAsyOpen(basicRemark)
    //    basicRemark = AttributesOperationCW.addfcdate(basicRemark, fcdate)
    //    basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
    //    basicRemark = AttributesOperationCW.addSourceDataType(basicRemark, Constant.Gr2bKey)
    //    basicRemark = AttributesOperationCW.addAppId(basicRemark, appId)
    //    val collectionflagStr: String = {
    //      if (scidata == null) {
    //        "N"
    //      } else {
    //        "Y"
    //      }
    //    }
    //    val sizeFlagStr: String = "Null"
    //    /*val arrFlagStr:String={
    //      val now:Date=new Date()
    //      if(now.after(fcdate)){
    //        "Y"
    //      }else{
    //        "N"
    //      }
    //    }*/
    //    val arrFlagStr: String = SendMsgCW.gr2bArrFlagStr(fcdate, fcType)
    //    val qualityFlagStr: String = "N"
    //    var stateRemark: JSONObject = new JSONObject( )
    //    stateRemark = AttributesOperationCW.addOtherMark(stateRemark, "此处质控仅表示检验数据源是否有大面积相同值(fcType=PRE不作处理)")
    //    SendMsgCW.collectionSend(gr2bfileName, changePerson, basicRemark, collectionflagStr, sizeFlagStr, arrFlagStr, qualityFlagStr, stateRemark)
    //---------------------------------------------------

    scidata
  }

  private def ReadGr2b (gr2bfileName: String): SciDatasetCW = {
    val scidata: SciDatasetCW = this.ReadNcScidata(gr2bfileName)
    scidata
  }

  def ReadNcScidata (fileName: String, vars: Array[String]): SciDatasetCW = {
    val varName: List[String] = {
      if (vars == null) {
        Nil
      } else {
        vars.toList
      }
    }
    this.read(fileName, varName)
  }

  def ReadNcScidata (fileName: String, vars: ArrayBuffer[String]): SciDatasetCW = {
    this.read(fileName, vars.toList)
  }

  def ReadNcRdd (path: String, jsonFile: String): RDD[SciDatasetCW] = {
    this.ReadNcRdd(path, Nil, jsonFile)
  }

  def ReadNcRdd (path: String, vars: ArrayBuffer[String], jsonFile: String): RDD[SciDatasetCW] = {
    val varsL: List[String] = {
      if (vars == null) {
        Nil
      } else {
        vars.toList
      }
    }
    this.ReadNcRdd(path, varsL, jsonFile)
  }

  private def ReadNcRdd (path: String, vars: List[String], jsonFile: String): RDD[SciDatasetCW] = {
    val ssc = ContextUtil.getSciSparkContextCW( )
    val splitNum = ShareData.splitNum_ExistOrCreat(jsonFile)
    ssc.netcdfWholeDatasets(path, Nil, splitNum)
  }

  def ReadNcRdd (path: String, vars: Array[String], jsonFile: String): RDD[SciDatasetCW] = {
    val varsL: List[String] = {
      if (vars == null) {
        Nil
      } else {
        vars.toList
      }
    }
    this.ReadNcRdd(path, varsL, jsonFile)
  }

  def ReadFcRdd (jsonFile: String, timeSteps: Array[Double], fcTypes: Array[String], fcdate: Date): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, timeSteps, fcTypes, Array(fcdate))
  }

  def ReadFcRdd (jsonFile: String, timeSteps: Array[Double], fcType: String, fcdates: Array[Date]): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, timeSteps, Array(fcType), fcdates)
  }

  def ReadFcRdd (jsonFile: String, timeStep: Double, fcTypes: Array[String], fcdates: Array[Date]): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, Array(timeStep), fcTypes, fcdates)
  }

  private def ReadFcRdd (jsonFile: String, timeSteps: Array[Double], fcTypes: Array[String], fcdates: Array[Date]): RDD[SciDatasetCW] = {
    this.ReadFcRdd0(jsonFile, timeSteps, fcTypes, fcdates)
  }

  private def ReadFcRdd0 (jsonFile: String, timeSteps: Array[Double], fcTypes: Array[String], fcdates: Array[Date]): RDD[SciDatasetCW] = {
    val ssc = ContextUtil.getSciSparkContextCW( )
    val jsonArr = ReadFile.ReadJsonArr(jsonFile)
    val splitNum = jsonArr.length
    fcdates.map(fcdate => {
      fcTypes.map(fcType => {
        timeSteps.map(timeStep => {
          jsonArr.map(
            jpro => {
              //              val idName:String=jpro.getString(AttributesOperationCW.idNameKey)
              val idName: String = Analysis.analysisJsonIdName(jpro)
              val fcfile: String = PropertiesUtil.getfcFile(timeStep, fcType, fcdate, idName)
              //              println(this.getClass.getCanonicalName+";idName="+idName+",timeStep="+timeStep+",fcType="+fcType)
              ssc.netcdfWholeDatasets(fcfile, Nil, splitNum)
            }
          ).reduce((x, y) => (x.union(y)))
        }).reduce((x, y) => (x.union(y)))
      }).reduce((x, y) => (x.union(y)))
      //    }).reduce((x,y)=>(x.union(y))).coalesce(splitNum,true)
    }).reduce((x, y) => (x.union(y)))
  }

  def ReadFcRdd (jsonFile: String, timeSteps: Array[Double], fcType: String, fcdate: Date): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, timeSteps, Array(fcType), Array(fcdate))
  }

  def ReadFcRdd (jsonFile: String, timeStep: Double, fcTypes: Array[String], fcdate: Date): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, Array(timeStep), fcTypes, Array(fcdate))
  }

  def ReadFcRdd (jsonFile: String, timeStep: Double, fcType: String, fcdates: Array[Date]): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, Array(timeStep), Array(fcType), fcdates)
  }

  def ReadFcRdd (jsonFile: String, timeStep: Double, fcType: String, fcdate: Date): RDD[SciDatasetCW] = {
    this.ReadFcRdd(jsonFile, Array(timeStep), Array(fcType), Array(fcdate))
  }

  def ReadFcComplete (jsonRdd: RDD[JSONObject], timeStep: Double, fcType: String, fcdate: Date): Boolean = {
    this.ReadFcExist(jsonRdd, timeStep, fcType, fcdate).map(f => f._1).reduce((x, y) => (x && y))
  }

  def ReadFcComplete (jsonArr: Array[JSONObject], timeStep: Double, fcType: String, fcdate: Date): Boolean = {
    this.ReadFcExist(jsonArr, timeStep, fcType, fcdate).map(f => f._1).reduce((x, y) => (x && y))
  }

  def ReadFcExist (jsonRdd: RDD[JSONObject], timeStep: Double, fcType: String, fcdate: Date): RDD[(Boolean, String, Double, String)] = {
    jsonRdd.map(
      jpro => {
        //        val idName=jpro.getString(AttributesOperationCW.idNameKey)
        val idName: String = Analysis.analysisJsonIdName(jpro)
        (this.ReadFcExist(idName, timeStep, fcType, fcdate), idName, timeStep, fcType)
      }
    )
  }

  def ReadFcExist (jsonArr: Array[JSONObject], timeStep: Double, fcType: String, fcdate: Date): Array[(Boolean, String, Double, String)] = {
    jsonArr.map(
      jpro => {
        //        val idName=jpro.getString(AttributesOperationCW.idNameKey)
        val idName: String = Analysis.analysisJsonIdName(jpro)
        (this.ReadFcExist(idName, timeStep, fcType, fcdate), idName, timeStep, fcType)
      }
    )
  }

  def ReadFcExist (idName: String, timeStep: Double, fcType: String, fcdate: Date): Boolean = {
    val fcfile = PropertiesUtil.getfcFile(timeStep, fcType, fcdate, idName)
    val existsflag = HDFSOperation1.exists(fcfile)
    if (existsflag) {
      val lengflag: Boolean = HDFSFile.filesizeB(fcfile) > 100
      if (lengflag) {
        true
      } else {
        val msg1 = fcfile + "文件小于100B"

        println(msg1)
        false
      }
    } else {
      val msg0 = fcfile + "不存在"
      val e: Exception = new Exception(msg0)
      e.printStackTrace( )
      //      println(msg0)
      false
    }
  }

  /**
    * 读取hdfs中的nc文件
    * 目前只支持nc格式
    */
  /*private def read(ssc: SciSparkContext, path: String, vars: List[String] = Nil, partitions : Int = -1): RDD[SciDataset] = {
    if(!HDFSOperation.exists(path)){
      val msg=path+"不存在,创建空目录,但返回RDD为空"
      HDFSOperation.createFolder(path)
      /*val e:Exception=new Exception(msg)
      e.printStackTrace()*/
      println(msg)
    }
    if(!HDFSOperation.hasChildrenFile(path)){
      val msg=path+"没有数据"
      /*val e:Exception=new Exception(msg)
      e.printStackTrace()*/
      println(msg)
    }
    val readStart=System.currentTimeMillis()
    val result=if (partitions > 0) {
      ssc.sciDatasets(path, vars, partitions)
    } else {
      ssc.sciDatasets(path, vars)
    }
    val readEnd=System.currentTimeMillis()

    WeatherShowtime.showDateStrOut1("readRDD",readStart,readEnd,"Path="+path)
    result
  }*/

  /*def ReadNcName(path:String):String={
    val holder : ListBuffer[String] = new ListBuffer[String]
    val paths : List[String] = HDFSOperation.listChildrenFile(path).toList
    val file:String=paths.apply(0)
    file.toString
  }*/

  private def ReadFcRdd1 (jsonRdd: RDD[JSONObject], timeSteps: Array[Double], fcTypes: Array[String], fcdates: Array[Date], idNameMaps: mutable.HashMap[String, Array[(String, Double)]]): RDD[SciDatasetCW] = {
    val ssc = ContextUtil.getSciSparkContextCW( )
    val jsonArr = jsonRdd.collect( )
    val splitNum = jsonArr.length
    fcdates.map(fcdate => {
      fcTypes.map(fcType => {
        timeSteps.map(timeStep => {
          jsonArr.map(
            jpro => {
              //              val idName:String=jpro.getString(AttributesOperationCW.idNameKey)
              val idName: String = Analysis.analysisJsonIdName(jpro)
              val fcfile: String = PropertiesUtil.getfcFile(timeStep, fcType, fcdate, idName)
              val groupStr: String = IdNameMap.groupKeyShowStr(idNameMaps, idName, timeStep)
              println(this.getClass.getCanonicalName + ";" + groupStr + ",fcType=" + fcType)
              //              ssc.netcdfWholeDatasetsTest(fcfile,idName,timeStep,fcType,Nil,splitNum)
              ssc.netcdfWholeDatasets(fcfile, Nil, splitNum)
            }
          ).reduce((x, y) => (x.union(y)))
        }).reduce((x, y) => (x.union(y)))
      }).reduce((x, y) => (x.union(y)))
      //    }).reduce((x,y)=>(x.union(y))).coalesce(splitNum,true)
    }).reduce((x, y) => (x.union(y)))
  }

  private def ReadFcRdd2 (jsonRdd: RDD[JSONObject], timeSteps: Array[Double], fcTypes: Array[String], fcdates: Array[Date], idNameMaps: mutable.HashMap[String, Array[(String, Double)]]): RDD[SciDatasetCW] = {
    val ssc = ContextUtil.getSciSparkContextCW( )
    val jsonArr = jsonRdd.collect( )
    val splitNum = jsonArr.length
    fcdates.map(fcdate => {
      fcTypes.map(fcType => {
        timeSteps.map(timeStep => {
          jsonRdd.map(
            jpro => {
              //              val idName:String=jpro.getString(AttributesOperationCW.idNameKey)
              val idName: String = Analysis.analysisJsonIdName(jpro)
              val fcfile: String = PropertiesUtil.getfcFile(timeStep, fcType, fcdate, idName)
              val groupStr: String = IdNameMap.groupKeyShowStr(idNameMaps, idName, timeStep)
              println(this.getClass.getCanonicalName + ";" + groupStr + ",fcType=" + fcType)

              this.ReadFcScidata(idName, timeStep, fcType, fcdate)
            }
          )
        }).reduce((x, y) => (x.union(y)))
      }).reduce((x, y) => (x.union(y)))
      //    }).reduce((x,y)=>(x.union(y))).coalesce(splitNum,true)
    }).reduce((x, y) => (x.union(y)))
  }

  def ReadFcScidata (idName: String, timeStep: Double, fcType: String, fcdate: Date): SciDatasetCW = {
    val FcNcFile: String = PropertiesUtil.getfcFile(timeStep, fcType, fcdate, idName)
    val filesize = HDFSFile.filesizeMB(FcNcFile)
    val bakmsg: String = "idName=" + idName + ",fcType=" + fcType + ",timeStep=" + timeStep + ",FcNcFile=" + FcNcFile + ",fileSize=" + filesize + "MB,ip=" + workersUtil.getIp( )
    val readStart = System.currentTimeMillis( )
    val scidata: SciDatasetCW = this.ReadNcScidata(FcNcFile)
    val readEnd = System.currentTimeMillis( )
    WeatherShowtime.showDateStrOut1("readScidata", readStart, readEnd, bakmsg)
    scidata
  }

  def ReadKVRddArrConf(splitFile: String, timeSteps: Array[Double], fcTypes: Array[String], fcdate: Date, idNameMaps: mutable.HashMap[String, Array[(String, Double)]]): RDD[(String, Iterable[((String, Double), Iterable[String])])] = {
    val splitNum = ShareData.splitNum_ExistOrCreat(splitFile)
    val sc = ContextUtil.getSparkContext( )
    //    val ssc=ContextUtil.getSciSparkContext()
    //    val split=splitNum+RddKVStore.partitionCoefficient
    val rdd: RDD[(String, Iterable[((String, Double), Iterable[String])])] = {
      val idNameArr: Array[(String, Array[(String, Double)])] = idNameMaps.toArray
      //      val groupRdd0: RDD[(String, Array[(String, Double)])] = sc.parallelize(idNameArr,idNameArr.length)
      val groupRdd0: RDD[(String, Array[(String, Double)])] = sc.makeRDD(idNameArr, idNameArr.length)
      val groupRdd1: RDD[(String, Iterable[((String, Double), Iterable[String])])] = groupRdd0.map(groupidNameTimeStep => {
        val groupKey = groupidNameTimeStep._1
        val groupValue: Iterable[((String, Double), Iterable[String])] = groupidNameTimeStep._2.toIterable.map(idNametimeStep => {
          val idName = idNametimeStep._1
          val timeStep = idNametimeStep._2
          ((idName, timeStep), fcTypes.toIterable)
        })
        (groupKey, groupValue)
      })
      groupRdd1
    }
    rdd
  }

}
