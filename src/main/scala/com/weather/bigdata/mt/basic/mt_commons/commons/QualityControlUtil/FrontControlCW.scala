package com.weather.bigdata.mt.basic.mt_commons.commons.QualityControlUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

object FrontControlCW {

  def frontControl(Rdd: RDD[SciDatasetCW]): Boolean = {
    Rdd.map(scidata => this.frontControl(scidata)).reduce((x, y) => (x || y))
  }

  def frontControl(scidata: SciDatasetCW): Boolean = {
    val start = System.currentTimeMillis()

    val fcType = AttributesOperationCW.getFcType(scidata)
    val (timeName: String, heightName: String, latName: String, lonName: String) = PropertiesUtil.getVarName()

    val timeV: VariableCW = scidata.apply(timeName)
    val heightV: VariableCW = scidata.apply(heightName)
    val latV: VariableCW = scidata.apply(latName)
    val lonV: VariableCW = scidata.apply(lonName)
    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()
    val length = timeLen * heightLen * latLen * lonLen
    val planelength = heightLen * latLen * lonLen

    val dataNames: Array[String] = PropertiesUtil.getrElement_rename(fcType)

    val flag: Boolean = dataNames.map(dataName => {
      val DataV = scidata.apply(dataName)
      val flag = new Array[Boolean](timeLen)
      var checkData: Set[Double] = Set()

      for (i <- 0 until length) {
        val tt = i / planelength
        var begeinLoc = 0
        var endLoc = 0
        if (i % planelength == 0) { //新的时次
          begeinLoc = i
          checkData = Set()
        }
        checkData += DataV.dataDouble(i)
        if (i % planelength == (planelength - 1)) { //时次结束点发布数据检查结果
          endLoc = i
          if (checkData.size <= 1) { //同一时次层数据一致即为异常
            if (dataName.equals(Constant.PRE) & checkData.last == 0) {
              //降水为0的情况除外
              flag(tt) = true
            } else {
              flag(tt) = false
            }
          } else {
            flag(tt) = true
          }

          if (flag(tt) == false) {
            val sciDataName = scidata.datasetName
            val msg = sciDataName + ":" + (tt + 1) * 3 + "时次检查完毕，不重复的格点数" + checkData.size + "，数据状态异常(单时次内出现所有数据相同则报异常,刨除PRE中单时次数据全0情况)"
            println(msg)
          }
        }


      } //今日数据检查结束


      val revisedNum = flag.filter(_ == false).length

      val idName = AttributesOperationCW.getIDName(scidata)
      println("前置质控FrontControl检查:idName=" + idName + ";" + dataName + "结束,共有" + revisedNum + "时次出现异常(单时次内出现所有数据相同则报异常,刨除PRE中单时次数据全0情况)")


      if (revisedNum > 0) {
        false
      } else {
        true
      }


    }).reduce((x, y) => (x && y))

    AttributesOperationCW.addTrace(scidata.attributes, "FrontControl")

    val end = System.currentTimeMillis()

    WeatherShowtime.showDateStrOut1("QualityControl.FrontControl:", start, end)

    flag
  }
}
