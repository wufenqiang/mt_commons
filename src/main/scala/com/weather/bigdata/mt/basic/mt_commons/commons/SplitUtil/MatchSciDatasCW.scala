package com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.VariableOperationCW
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil.IdNameMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MatchSciDatasCW {
  //  private val scalaAO=new ArrayOperation
  private val VO = VariableOperationCW
  private val AttrOp = AttributesOperationCW

  def fuse0(arrRdd: Array[RDD[SciDatasetCW]], timeName: String, heightName: String, latName: String, lonName: String): RDD[SciDatasetCW] = {
    if (arrRdd.length == 1) {
      arrRdd(0)
    } else {
      val Rdds: RDD[SciDatasetCW] = arrRdd.reduce(
        (x, y) => {
          val rdd: RDD[SciDatasetCW] = {
            val rddx_y: RDD[(SciDatasetCW, SciDatasetCW)] = this.matchByName0(x, y)
            val rddxy: RDD[SciDatasetCW] = this.scidatafuse0(rddx_y, timeName, heightName, latName, lonName)
            rddxy
          }
          rdd
        }
      )
      Rdds
    }
  }

  private def scidatafuse0(rdds: RDD[(SciDatasetCW, SciDatasetCW)], timeName: String, heightName: String, latName: String, lonName: String): RDD[SciDatasetCW] = {
    val rdd: RDD[SciDatasetCW] = rdds.map(
      scidatasets => {
        val scidataset0 = scidatasets._1
        val scidataset1 = scidatasets._2

        val idname1 = AttrOp.getIDName(scidataset0)
        val idname2 = AttrOp.getIDName(scidataset1)

        if (idname1.equals(idname2)) {
          println("idname1=" + idname1 + ";idname2=" + idname2)
        } else {
          val e: Exception = new Exception("scidatafuse0,传入数据出错,idname不匹配")
          e.printStackTrace()
        }

        val names = Array(timeName, heightName, latName, lonName)
        val mainV: ArrayBuffer[VariableCW] = VariableOperationCW.getVars(scidataset0.variables.values, names)
        val mainV0 = VariableOperationCW.plusOtherVars(mainV, scidataset0, names)
        val mainV01: ArrayBuffer[VariableCW] = VariableOperationCW.plusOtherVars(mainV0, scidataset1, names)
        val sciname0 = scidataset0.datasetName
        val sciname1 = scidataset1.datasetName
        val dataName_new = sciname0 + "_" + sciname1
        //        val varHash=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idname1,mainV01,(timeName,heightName,latName,lonName))
        val att0: mutable.HashMap[String, String] = scidataset0.attributes
        val att1: mutable.HashMap[String, String] = scidataset1.attributes
        val att: mutable.HashMap[String, String] = att1.++(att0)

        new SciDatasetCW(mainV01, att, dataName_new)
      }
    )
    rdd
  }

  def matchByName0(rdd0: RDD[SciDatasetCW], rdd1: RDD[SciDatasetCW]): RDD[(SciDatasetCW, SciDatasetCW)] = {
    val rddKV: RDD[(String, Iterable[SciDatasetCW])] = this.GroupByName2NameIterable(rdd0, rdd1)
    val rdd: RDD[(SciDatasetCW, SciDatasetCW)] = rddKV.map(Iter => {
      val name = Iter._1
      val list = Iter._2.toList
      if (list.length != 2) {
        val e: Exception = new Exception("Name=" + name + ",block=" + list.length + ";找不到对应数据块,请查看数据源中的对应Name")
        e.printStackTrace()
        //          println("Name="+name+","+list.length)
      }
      (list(0), list(1))
    })
    rdd
  }

  private def GroupByName2NameIterable(rdd0: RDD[SciDatasetCW], rdd1: RDD[SciDatasetCW]): RDD[(String, Iterable[SciDatasetCW])] = {
    rdd0.union(rdd1).groupBy(f => AttrOp.getIDName(f))
  }

  def matchByName4(arrRdd: Iterable[RDD[SciDatasetCW]], jsonArr: Array[JSONObject]): RDD[(String, Iterable[((String, Double), Iterable[SciDatasetCW])])] = {
    val splitNum: Int = jsonArr.length
    val unionrdd: RDD[SciDatasetCW] = arrRdd.filter(f => (f != null)).reduce((x, y) => x.union(y))
    val rddKV0: RDD[((String, Double), Iterable[SciDatasetCW])] = unionrdd.groupBy(f => {
      val idName = AttributesOperationCW.getIDName(f)
      val timeStep = AttributesOperationCW.gettimeStep(f)
      (idName, timeStep)
    })
    val rddKV1: RDD[(String, Iterable[((String, Double), Iterable[SciDatasetCW])])] = rddKV0.groupBy(f => {
      val idName = f._1._1
      val timeStep = f._1._2
      idName + "_" + timeStep
    }).coalesce(splitNum, true)
    rddKV1
  }

  def matchByName2(arrRdd: Iterable[RDD[SciDatasetCW]], jsonArr: Array[JSONObject]): RDD[((String, Double), Iterable[((String, Double), Iterable[SciDatasetCW])])] = {
    val splitNum: Int = jsonArr.length
    val unionrdd: RDD[SciDatasetCW] = arrRdd.filter(f => (f != null)).reduce((x, y) => x.union(y))
    val rddKV: RDD[((String, Double), Iterable[((String, Double), Iterable[SciDatasetCW])])] = unionrdd.groupBy(f => {
      val idName = AttributesOperationCW.getIDName(f)
      val timeStep = AttrOp.gettimeStep(f)
      (idName, timeStep)
    }).groupBy(f => f._1).coalesce(splitNum, true)

    rddKV
  }

  def matchByName3(idNameMaps: mutable.HashMap[String, Array[(String, Double)]], arrRdd: Iterable[RDD[SciDatasetCW]], jsonFile: String): RDD[(String, Iterable[((String, Double), Iterable[SciDatasetCW])])] = {
    val jsonArr = ShareData.jsonArr_ExistOrCreat(jsonFile)
    val splitNum: Int = ShareData.splitNum_ExistOrCreat(jsonFile)
    //    val partitionCoefficient:Int=RddKVStore.partitionCoefficient
    val unionrdd: RDD[SciDatasetCW] = arrRdd.filter(f => (f != null)).reduce((x, y) => x.union(y)).coalesce(splitNum, true)
    val rddKV0: RDD[((String, Double), Iterable[SciDatasetCW])] = unionrdd.groupBy(f => {
      val idName: String = AttributesOperationCW.getIDName(f)
      val timeStep: Double = AttrOp.gettimeStep(f)
      (idName, timeStep)
    })
    val rdd1: RDD[(String, Iterable[((String, Double), Iterable[SciDatasetCW])])] = rddKV0.groupBy(f => {
      val idName = f._1._1
      val timeStep = f._1._2
      val key = IdNameMap.getidNameKey1(idNameMaps, idName, timeStep)
      key
    })
      .coalesce(idNameMaps.size, true)
    rdd1
  }

  def rdd2rddKV(rdd: RDD[SciDatasetCW]): RDD[(String, SciDatasetCW)] = rdd.keyBy(f => AttrOp.getIDName(f))

  def rdd2rddName(rdd: RDD[SciDatasetCW]): RDD[String] = {
    val rddName: RDD[String] = {
      rdd.map(
        scidata => {
          AttrOp.getIDName(scidata)
        }
      )
    }
    rddName
  }

  def main(args: Array[String]): Unit = {
    /*val jsonFile=PropertiesUtil.getjsonFile()
    val jsonArr:Array[JSONObject]=PropertiesUtil.getJsonArr(jsonFile)
    val idNames:Array[String]=jsonArr.map(j=>AccordSciDatas_JsonObject.analysisJsonIdName(j))
    val timeSteps:Array[Double]=Array(1.0d,12.0d)
    val set0:mutable.HashSet[String]=new mutable.HashSet[String]()

    val idNameMaps=this.getidNameMap(timeSteps,jsonArr)
    idNameMaps.foreach(f=>{
      val groupKey=f._1
      val idNameTimeStep=f._2
      idNameTimeStep.foreach(g=>{
        val idName=g._1
        val timeStep=g._2
        val groupKeyStr=this.groupKeyShowStr(idNameMaps,idName,timeStep)
        val msg="groupKey="+groupKey+";"+groupKeyStr
        println(msg)
      })
    })
    /*println()
    idNames.foreach(idName=>{
      timeSteps.foreach(timeStep=>{
        val key0=this.idNameTimeStep2Key1(idNameMaps,idName,timeStep)
        val key1=this.idNameTimeStep2Key1(idNameMaps,idName,timeStep)
        println("idName="+idName+",timeStep="+timeStep+"==>"+key0+","+key1)
        set0.add(key0)
      })
    })*/
    //    println()
    println("idNames.size="+idNames.size+";idNameMaps.size="+idNameMaps.size)*/
  }


  /*private def getidNameMap:mutable.HashMap[String,Array[String]]={
    val idNameArr_Stable:Array[String]=PropertiesUtil.getJsonArr.map(json0=>AccordSciDatas_JsonObject.analysisJsonIdName(json0))
    val idNameMap0:mutable.HashMap[String,Array[String]]=ArrayOperation.randomGroup(idNameArr_Stable,RddKVStore.partitionCoefficient)
    idNameMap0
  }*/

  //  //控制分区HashMap
  //  def getidNameMap(timeSteps:Array[Double],jsonFile:String):mutable.HashMap[String,Array[(String,Double)]]={
  //    val jsonArr: Array[JSONObject] = ShareData.jsonArr_ExistOrCreat(jsonFile)
  //    val splitNum=jsonArr.length
  //    val partitionCoefficient:Int=sparkJobSubmit.partitionCoefficient(splitNum)
  //
  //    val idNameArr_Stable:Array[(String,Double)]=timeSteps.filter(timeStep=>timeStep==12.0d).map(timeStep=>jsonArr.map(json0=>{
  //      val idName=AccordSciDatas_JsonObject.analysisJsonIdName(json0)
  //      (idName,timeStep)
  //    })).reduce((x,y)=>(x.union(y)))
  //    val idNameMap0:mutable.HashMap[String,Array[(String,Double)]]=ArrayOperation.randomGroup(idNameArr_Stable,partitionCoefficient)
  //
  //    timeSteps.filter(timeStep=>timeStep==1.0d).foreach(timeStep=>{
  //      jsonArr.foreach(json0=>{
  //        val idName=AccordSciDatas_JsonObject.analysisJsonIdName(json0)
  //        val groupKey=idName+"_"+timeStep
  //        idNameMap0.put(groupKey,Array((idName,timeStep)))
  //      })
  //    })
  //
  //    //显示信息无用可注释
  //    idNameMap0.foreach(f => {
  //      val groupKey = f._1
  //      val idNameTimeStep = f._2
  //      idNameTimeStep.foreach(g => {
  //        val idName = g._1
  //        val timeStep = g._2
  //        val groupKeyStr = this.groupKeyShowStr(idNameMap0, idName, timeStep)
  //        val msg = "groupKey=" + groupKey + ";" + groupKeyStr
  //        println(msg)
  //      })
  //    })
  //    println("idNameMaps.size=" + idNameMap0.size)
  //
  //
  //    idNameMap0
  //  }
  //
  //
  //  /*private def getidNameKey0(idNameMap:mutable.HashMap[String,Array[String]],idName:String): String ={
  //    /*val JsonArr:Array[JSONObject]=PropertiesUtil.getJsonArr
  //    val idNameArr:Array[String]=JsonArr.map(json0=>AccordSciDatas_JsonObject.analysisJsonProperties(json0)._1)
  ////    val idNameMap:mutable.HashMap[String,Array[String]]=ArrayOperation.remainderGroup(idNameArr,partitionCoefficient)
  //    val idNameMap:mutable.HashMap[String,Array[String]]=ArrayOperation.randomGroup(idNameArr,partitionCoefficient)*/
  //    var key:String=""
  //    idNameMap.foreach(f=>{
  //      val key0:String=f._1
  //      val value0:Array[String]=f._2
  //      if(value0.contains(idName)){
  //        key =key0
  //      }
  //    })
  //    key
  //  }*/
  //
  //  private def getidNameKey1(idNameMap:mutable.HashMap[String,Array[(String,Double)]],idName:String,timeStep:Double): String ={
  //    var key:String=""
  //    idNameMap.foreach(f=>{
  //      val key0:String=f._1
  //      val value0:Array[(String,Double)]=f._2
  //      if(value0.contains((idName,timeStep))){
  //        key =key0
  //      }
  //    })
  //    key
  //  }
  //
  //  /*def idNameTimeStep2Key0(idNameMap:mutable.HashMap[String,Array[String]],idName:String,timeStep:Double): String ={
  //    val key:String={
  //      if(timeStep==1.0d){
  //        idName+"_"+timeStep
  //      }else if(timeStep==12.0d||timeStep==24.0d){
  //        this.getidNameKey0(idNameMap,idName)
  //      }else{
  //        val msg="idNameTimeStep2Key,timeStep="+timeStep+"未配置"
  //        println(msg)
  //        ""
  //      }
  //    }
  //    if(key.equals("")){
  //      val msg:String="idNameTimeStep2Key0出错;idName="+idName+",timeStep="+timeStep
  //      println(msg)
  //    }
  //    key
  //  }*/
  //  def groupKey(idNameMap:mutable.HashMap[String,Array[(String,Double)]], idName:String, timeStep:Double): String ={
  //    val key=this.getidNameKey1(idNameMap,idName,timeStep)
  //    if("".equals(key)){
  //      val msg="groupKey,timeStep="+timeStep+",idName="+idName+"未配置"
  //      println(msg)
  //    }
  //    key
  //  }
  //
  //  /*def idNameTimeStep2KeyShowStr0(idNameMap:mutable.HashMap[String,Array[String]], idName:String, timeStep:Double): String ={
  //    val groupKey=this.idNameTimeStep2Key0(idNameMap,idName,timeStep)
  //    "groupKey="+groupKey+"(idName="+idName+",timeStep="+timeStep+",ip="+SparkUtil.getIp()+",ExecutorId="+SparkUtil.getExecutorId()+")"
  //  }*/
  //  def groupKeyShowStr(idNameMaps:mutable.HashMap[String,Array[(String,Double)]], idName:String, timeStep:Double): String ={
  //    val groupKey=this.groupKey(idNameMaps,idName,timeStep)
  //    "groupKey=" + groupKey + ";idName=" + idName + ";timeStep=" + timeStep + ";host=" + workersUtil.getHostName( ) + "(" + workersUtil.getIp( ) + ");ExecutorId=" + workersUtil.getExecutorId( )
  //  }

  //有bug,计算后无数据
  private def fuse1(arrRdd: Array[RDD[SciDatasetCW]], jsonFile: String, timeName: String, heightName: String, latName: String, lonName: String): RDD[SciDatasetCW] = {
    if (arrRdd.length == 1) {
      arrRdd(0)
    } else {
      val arrRddKV: RDD[(String, Iterable[SciDatasetCW])] = this.matchByName1(arrRdd, jsonFile)
      val rdd: RDD[SciDatasetCW] = arrRddKV.map(RDDKV => this.Iterfuse(RDDKV._2, timeName, heightName, latName, lonName))
      rdd
    }
  }

  private def Iterfuse(iterScidata: Iterable[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String): SciDatasetCW = {
    val scidatas: SciDatasetCW = iterScidata.reduce(
      (x, y) => {
        val idnamex = AttrOp.getIDName(x)
        val idnamey = AttrOp.getIDName(y)
        if (idnamex == idnamey) {
          val xtime: Array[Double] = x.apply(timeName).dataDouble()
          val ytime: Array[Double] = y.apply(timeName).dataDouble()
          if (ArrayOperation.ArrayEqual(xtime, ytime)) {
            val xheight: Array[Double] = x.apply(heightName).dataDouble()
            val yheight: Array[Double] = y.apply(heightName).dataDouble()
            if (ArrayOperation.ArrayEqual(xheight, yheight)) {
              val xlat: Array[Double] = x.apply(latName).dataDouble()
              val ylat: Array[Double] = y.apply(latName).dataDouble()
              if (ArrayOperation.ArrayEqual(xlat, ylat)) {
                val xlon: Array[Double] = x.apply(lonName).dataDouble()
                val ylon: Array[Double] = y.apply(lonName).dataDouble()
                if (ArrayOperation.ArrayEqual(xlon, ylon)) {
                  val Names = Array(timeName, heightName, latName, lonName)
                  val vars = new ArrayBuffer[VariableCW]
                  val timeV = x.apply(timeName)
                  val heightV = x.apply(heightName)
                  val latV = x.apply(latName)
                  val lonV = x.apply(lonName)
                  val xotherV = VO.plusOtherVars(null, x, Names)
                  val yotherV = VO.plusOtherVars(null, y, Names)
                  vars.++(xotherV).++(yotherV)
                  vars += timeV += heightV += latV += lonV
                  val attr = AttrOp.fuseAttribute(x.attributes, y.attributes)
                  val datasetName = x.datasetName + "_" + y.datasetName
                  //                  val varHash=VO.allVars2NameVar4(this.getClass.toString,idnamex,vars,(timeName,heightName,latName,lonName))
                  new SciDatasetCW(vars, attr, datasetName)
                } else {
                  val e: Exception = new Exception("Iterfuse,lon不匹配")
                  e.printStackTrace()
                  null
                }
              } else {
                val e: Exception = new Exception("Iterfuse,lat不匹配")
                e.printStackTrace()
                null
              }
            } else {
              val e: Exception = new Exception("Iterfuse,height不匹配")
              e.printStackTrace()
              null
            }
          } else {
            val e: Exception = new Exception("Iterfuse,time不匹配")
            e.printStackTrace()
            null
          }
        } else {
          println("Iterfuse数据传入出错,idname不匹配")
          null
        }
      }
    )
    scidatas
  }

  def matchByName1(arrRdd: Iterable[RDD[SciDatasetCW]], jsonFile: String): RDD[(String, Iterable[SciDatasetCW])] = {
    val splitNum: Int = ShareData.splitNum_ExistOrCreat(jsonFile)
    val unionrdd: RDD[SciDatasetCW] = arrRdd.filter(f => (f != null)).reduce((x, y) => x.union(y))
    val rddKV: RDD[(String, Iterable[SciDatasetCW])] = unionrdd.groupBy(f => AttributesOperationCW.getIDName(f)).coalesce(splitNum, true)

    rddKV
  }

  private def scidatafuse1(rdds: RDD[Iterable[SciDatasetCW]], timeName: String, heightName: String, latName: String, lonName: String): RDD[SciDatasetCW] = {
    val rdd: RDD[SciDatasetCW] = rdds.map(
      Iterscidata => this.Iterfuse(Iterscidata, timeName, heightName, latName, lonName)
    )
    rdd
  }
}
