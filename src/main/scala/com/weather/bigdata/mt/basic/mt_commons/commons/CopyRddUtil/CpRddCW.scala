package com.weather.bigdata.mt.basic.mt_commons.commons.CopyRddUtil

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.SplitMatchInfo
import org.apache.spark.rdd.RDD

object CpRddCW {
  def cprddonlytimeStep(rdds_hasFcTypes: RDD[SciDatasetCW], timeSteps: Array[Double]): RDD[SciDatasetCW] = {
    val cprdd: RDD[SciDatasetCW] = timeSteps.map(
      timeStep => {
        rdds_hasFcTypes.map(
          scidata => {
            val attr = AttributesOperationCW.updateTimeStep(scidata.attributes, timeStep)
            //            val newSci=new SciDatasetCW()(scidata.variables,attr,scidata.datasetName)
            val newSci: SciDatasetCW = scidata.clone()
            newSci
          }
        )
      }
    ).reduce((x, y) => (x.union(y)))
    cprdd
  }

  def cprddonlyfctype(rdds_hasTimeSteps: RDD[SciDatasetCW], fcTypes: Array[String]): RDD[((String, Double, String), SciDatasetCW)] = {
    val cprdd: RDD[((String, Double, String), SciDatasetCW)] = fcTypes.map(
      fcType => {
        rdds_hasTimeSteps.map(
          scidata => {
            val idName: String = AttributesOperationCW.getIDName(scidata)
            val timeStep: Double = AttributesOperationCW.gettimeStep(scidata)
            //            val attr=AttributesOperation.coverInfo(scidata.attributes,AttributesOperation.fcTypeKey,fcType)
            val attr = AttributesOperationCW.updateFcType(scidata.attributes, fcType)
            val newSci = scidata.clone().asInstanceOf[SciDatasetCW]
            ((idName, timeStep, fcType), newSci)
          }
        )
      }
    ).reduce((x, y) => (x.union(y)))
    cprdd
  }

  def cprddbytimeStep(jsonRdds: (RDD[JSONObject], Int), rdd: RDD[SciDatasetCW], timeSteps: Array[Double], fcType: String): RDD[((String, Double, String), SciDatasetCW)] = {
    var rdd0 = timeSteps.map(timeStep => this.cprddbytimesStepfcType(rdd, timeStep, fcType)).reduce((x, y) => x.union(y))
    //    val splitNum=PropertiesUtil.getSplitNum
    //    rdd0 = rdd0.repartition(splitNum*timeSteps.length)
    rdd0
  }

  def cprddbyfcType(jsonRdds: (RDD[JSONObject], Int), rdd: RDD[SciDatasetCW], timeStep: Double, fcTypes: Array[String]): RDD[((String, Double, String), SciDatasetCW)] = {
    var rdd0 = fcTypes.map(fcType => this.cprddbytimesStepfcType(rdd, timeStep, fcType)).reduce((x, y) => x.union(y))
    //    val splitNum=PropertiesUtil.getSplitNum
    //    rdd0 = rdd0.repartition(splitNum*fcTypes.length)
    rdd0
  }

  def cprddbyTF(jsonRdds: (RDD[JSONObject], Int), rdd: RDD[SciDatasetCW], timeSteps: Array[Double], fcTypes: Array[String]): RDD[((String, Double, String), SciDatasetCW)] = {
    var rdd0 = timeSteps.map(timeStep => fcTypes.map(fcType => this.cprddbytimesStepfcType(rdd, timeStep, fcType))).reduce((x, y) => (x.union(y))).reduce((x, y) => (x.union(y)))
    //    val splitNum=PropertiesUtil.getSplitNum
    //    rdd0 = rdd0.repartition(splitNum*timeSteps.length*fcTypes.length)
    rdd0
  }

  //core
  def cprddbytimesStepfcType(rdd: RDD[SciDatasetCW], timeStep: Double, fcType: String): RDD[((String, Double, String), SciDatasetCW)] = {
    val rdds = rdd.map(
      scidata => {
        //        val attr0=AttributesOperation.coverInfo(scidata.attributes,AttributesOperation.timeStepKey,timeStep.toString)
        var attr0 = AttributesOperationCW.updateTimeStep(scidata.attributes, timeStep)
        attr0 = AttributesOperationCW.updateFcType(attr0, fcType)
        //        val attrnew=AttributesOperation.coverInfo(attr0,AttributesOperation.fcTypeKey,fcType)
        /*        val dataName=scidata.datasetName.replace(".nc","")
               val datasetName=dataName+"_"+timeStep.toString+".nc"*/
        val idName = AttributesOperationCW.getIDName(scidata)
        val datasetName = SplitMatchInfo.oldName2newName(fcType, idName, timeStep)
        //        new SciDataset(scidata.variables,attrnew,datasetName)
        val newscidata: SciDatasetCW = scidata.clone()
        newscidata.updataAttributes(attr0)
        newscidata.setName(datasetName)
        newscidata
      }
    )
    rdds.keyBy(
      scidata => {
        val att = scidata.attributes
        val idName = AttributesOperationCW.getIDName(scidata)
        val timeStep = AttributesOperationCW.gettimeStep(scidata)
        val fcType: String = AttributesOperationCW.getFcType(scidata)
        (idName, timeStep, fcType)
      }
    )
  }

  def cprddtimeSteps(jsonRdds: (RDD[JSONObject], Int), rdd: RDD[SciDatasetCW], timeSteps: Array[Double]): RDD[((String, Double), SciDatasetCW)] = {
    var rdd0 = timeSteps.map(timeStep => this.cprddtimeStep(rdd, timeStep)).reduce((x, y) => (x.union(y)))
    //    val splitNum=PropertiesUtil.getSplitNum
    //    rdd0 = rdd0.repartition(splitNum*timeSteps.length)
    rdd0
  }

  def cprddtimeStep(rdd: RDD[SciDatasetCW], timeStep: Double): RDD[((String, Double), SciDatasetCW)] = {
    rdd.map(scidata => {
      val idName = AttributesOperationCW.getIDName(scidata)
      //      scidata.attributes.update(AttributesOperation.timeStepKey,timeStep.toString)
      AttributesOperationCW.updateTimeStep(scidata, timeStep)
      ((idName, timeStep), scidata)
    })
  }

  def cprddfcTypes(jsonRdds: (RDD[JSONObject], Int), rdd: RDD[SciDatasetCW], fcTypes: Array[String]): RDD[((String, String), SciDatasetCW)] = {
    var rdd0 = fcTypes.map(fcType => this.cprddfcType(rdd, fcType)).reduce((x, y) => (x.union(y)))
    //    val splitNum=PropertiesUtil.getSplitNum
    //    rdd0 = rdd0.repartition(splitNum*fcTypes.length)
    rdd0
  }

  def cprddfcType(rdd: RDD[SciDatasetCW], fcType: String): RDD[((String, String), SciDatasetCW)] = {
    rdd.map(scidata => {
      val idName = AttributesOperationCW.getIDName(scidata)
      //      scidata.attributes.update(AttributesOperation.fcTypeKey,fcType)
      AttributesOperationCW.updateFcType(scidata.attributes, fcType)
      ((idName, fcType), scidata)
    })
  }

  def cpJsontimeSteps(jsonRdd: RDD[JSONObject], timeSteps: Array[Double]): RDD[(String, Double)] = {
    var rdd0 = timeSteps.map(timeStep => this.cpJsontimeStep(jsonRdd, timeStep)).reduce((x, y) => (x.union(y)))
    //    val splitNum=jsonRdds._2
    //    rdd0 = rdd0.repartition(splitNum*timeSteps.length)
    rdd0
  }

  private def cpJsontimeStep(jsonRdd: RDD[JSONObject], timeStep: Double): RDD[(String, Double)] = {
    jsonRdd.map(json => {
      val idName = Analysis.analysisJsonIdName(json)
      (idName, timeStep)
    })
  }

  def main(args: Array[String]): Unit = {
    /*val FcType=ConstantUtil.PRE
    val date:Date=DataFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
    val input=PropertiesUtil.getgr2bPath(FcType,date)
    val timeSteps=Array(1.0d,12.0d,3.0d)
    val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
    val scidata=ReadFile.ReadNcRdd(input).first()
    val (jsonRdd,splitNum)=PropertiesUtil.getJsonRdd
    val rdd=AccordSciDatas_JsonObject.AccSciRdd_reName(jsonRdd,scidata,timeName,heightName,latName,lonName,FcType,date)
    val cpRdd=this.cprddbytimeStep(rdd,timeSteps,FcType)
    cpRdd.foreach(f=>{
      val id=f._1._1
      val timeStep=f._1._2
      val scidata=f._2
      val outPath=PropertiesUtil.getfcPath(timeStep,FcType,date)

      WriteFile.WriteNcFile(scidata,outPath)
    })*/
  }
}
