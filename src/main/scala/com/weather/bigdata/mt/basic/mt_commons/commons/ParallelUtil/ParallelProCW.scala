package com.weather.bigdata.mt.basic.mt_commons.commons.ParallelUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.SplitMatchInfo
import org.apache.spark.rdd.RDD

object ParallelProCW {
  private val ws = new WeatherShowtime

  def cprddbytimeStep(rdd: RDD[SciDatasetCW], timeSteps: Array[Double], fcType: String): RDD[SciDatasetCW] = {
    //    val splitNum=PropertiesUtil.splitNum
    timeSteps.map(timeStep => {
      rdd.map(
        scidata => {
          val startTime = System.currentTimeMillis()

          //          val attr0=AttributesOperation.coverInfo(scidata.attributes,AttributesOperation.timeStepKey,timeStep.toString)
          //          val attrnew=AttributesOperation.coverInfo(attr0,AttributesOperation.fcTypeKey,fcType)
          var attrnew = AttributesOperationCW.updateTimeStep(scidata.attributes, timeStep)
          attrnew = AttributesOperationCW.updateFcType(attrnew, fcType)
          val idName = AttributesOperationCW.getIDName(scidata)
          val datasetName = SplitMatchInfo.oldName2newName(fcType, idName, timeStep)
          //          val scidata0=new SciDataset(scidata.variables,attrnew,datasetName)
          val scidata0 = scidata.clone()
          scidata0.setName(datasetName)
          scidata0.updataAttributes(attrnew)
          val endTime = System.currentTimeMillis()
          //--------------------日志采集----------------------------------

          //          val fileList = ""
          //          var basicRemark: JSONObject = new JSONObject()
          //          basicRemark = AttributesOperationCW.addFcType(basicRemark, fcType)
          //          basicRemark = AttributesOperationCW.addIDName(basicRemark, idName)
          //          basicRemark = AttributesOperationCW.addOtherMark(basicRemark, "cprddbytimeStep")
          //          val processFlagStr: String = SendMsgCW.flagStr_Scidata(scidata0)
          //          val qualityFlagStr: String = "N"
          //          var stateRemark: JSONObject = new JSONObject()
          //          val stateDetailRemark: JSONObject = new JSONObject()
          //          SendMsgCW.processSend(fileList, startTime, endTime, basicRemark, processFlagStr, qualityFlagStr, stateRemark, stateDetailRemark)
          //--------------------------------------------------------------

          /*val msg=ws.showDateStr("cprddbytimeStep:",startTime,endTime)
          println(msg)*/

          WeatherShowtime.showDateStrOut1("cprddbytimeStep:", startTime, endTime)

          scidata0
        }
      )
    }).reduce((x, y) => x.union(y)) /*.repartition(splitNum)*/
  }
}
