package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil.TimeInterpolationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import org.apache.spark.rdd.RDD

object KVStoreInterpolationCW {
  def twelve2twenty(fcRdds:Array[RDD[SciDatasetCW]]):Array[RDD[SciDatasetCW]]={
    fcRdds.map(rdd=>this.twelve2twenty(rdd))
  }
  def twelve2twenty(rdd:RDD[SciDatasetCW]): RDD[SciDatasetCW] ={
    if(rdd==null){
      null
    }else{
      rdd.map(scidata=>this.twelve2twenty(scidata))
    }
  }
  def twelve2twenty(scidata:SciDatasetCW): SciDatasetCW ={
    if(scidata==null){
      null
    }else{
      val(timeName,heightName,latName,lonName)=AttributesOperationCW.getVarName(scidata)
      val varsName = (timeName, heightName, latName, lonName)
      val fcType=AttributesOperationCW.getFcType(scidata)
      val timeStep=AttributesOperationCW.gettimeStep(scidata)
      val timeV=scidata.apply(timeName)
      val time=timeV.dataFloat()
      val times:Array[Float]=time.filter(p=>(p % 24.0f==0))
      val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)

      TimeInterpolationCW.timeIA(scidata, varsName, dataNames_alTypetimeIAs, times)
    }
  }
  def main(args:Array[String]): Unit ={
    val time=Array.range(12,240+12,12)
    val times=time.filter(p=>(p%24==0))
    println()
  }
}
