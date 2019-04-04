package com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{IndexOperation, VariableOperationCW}
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.{ArrayOperation, DateOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.Constant
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadObsCW
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object NcRevisedByObsCW {
  private val Kn: Int = 2

  def Obs(fcRdd: RDD[SciDatasetCW], ObsData: SciDatasetCW, dataName: String, latName_Obs: String, lonName_Obs: String, dataName_Obs: String, obsdate: Date): RDD[SciDatasetCW] = {
    val ReviseRdd = fcRdd.map(
      fc => {
        this.Obs(fc, ObsData, dataName, latName_Obs, lonName_Obs, dataName_Obs, obsdate)
      }
    )
    ReviseRdd
  }

  def Obs(sciData: SciDatasetCW, ObsData: SciDatasetCW, dataName: String, latName_Obs: String, lonName_Obs: String, dataName_Obs: String, obsdate: Date): SciDatasetCW = {
    val Start = System.currentTimeMillis()
    val idname = AttributesOperationCW.getIDName(sciData)
    val fcdate = AttributesOperationCW.getfcdate(sciData)
    val (timeName: String, heightName: String, latName: String, lonName: String) = AttributesOperationCW.getVarName(sciData)
    val fcType: String = AttributesOperationCW.getFcType(sciData)

    val obstime: Float = DateOperation.dHours(fcdate, obsdate)

    val obsdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)


    val mainV: ArrayBuffer[VariableCW] = this.ObsRevise_GetMainV(sciData, ObsData, fcType, timeName, heightName, latName, lonName, dataName, latName_Obs, lonName_Obs, dataName_Obs, obstime)
    val ObsReviseMain_All = VariableOperationCW.plusOtherVars(mainV, sciData, Array(timeName, heightName, latName, lonName, dataName))
    val datasetName = sciData.datasetName
    val attrs = sciData.attributes
    var attrs_new = AttributesOperationCW.addTrace(attrs, "Revise.Obs[obstime=" + obstime + ";obsdate=" + obsdateStr + ";fcType=" + fcType + "]")
    val scidatanew = new SciDatasetCW(mainV, attrs_new, datasetName)
    val End = System.currentTimeMillis()

    WeatherShowtime.showDateStrOut1("Obs", Start, End, "RDD[" + datasetName + "]")
    scidatanew
  }

  private def ObsRevise_GetMainV(sciData: SciDatasetCW, ObsData: SciDatasetCW, fcType: String, timeName: String, heightName: String, latName: String, lonName: String, dataName: String, latName_Obs: String, lonName_Obs: String, dataName_Obs: String, obstime: Float): ArrayBuffer[VariableCW] = {
    val vars: ArrayBuffer[VariableCW] = new ArrayBuffer[VariableCW]()

    val timeV: VariableCW = sciData.apply(timeName)
    val heightV: VariableCW = sciData.apply(heightName)
    val latV: VariableCW = sciData.apply(latName)
    val lonV: VariableCW = sciData.apply(lonName)
    val dataV: VariableCW = sciData.apply(dataName)

    val latV_Obs: VariableCW = ObsData.apply(latName_Obs)
    val lonV_Obs: VariableCW = ObsData.apply(lonName_Obs)
    val dataV_Obs: VariableCW = ObsData.apply(dataName_Obs)


    val AfterObs_dataV: VariableCW = this.ObsReviseData(fcType, timeV, heightV, latV, lonV, dataV, latV_Obs, lonV_Obs, dataV_Obs, obstime)

    vars += timeV += heightV += latV += lonV += AfterObs_dataV

    vars
  }

  private def ObsReviseData(fcType: String, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, dataV: VariableCW, latV_Obs: VariableCW, lonV_Obs: VariableCW, dataV_Obs: VariableCW, obstime: Float): VariableCW = {


    val lat: Array[Float] = NumOperation.Kndec(latV.dataFloat(), this.Kn)
    val lon: Array[Float] = NumOperation.Kndec(lonV.dataFloat(), this.Kn)

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    val Obs_lat: Array[Float] = NumOperation.Kndec(latV_Obs.dataFloat(), this.Kn)
    val Obs_lon: Array[Float] = NumOperation.Kndec(lonV_Obs.dataFloat(), this.Kn)

    val Obs_latLen = Obs_lat.length
    val Obs_lonLen = Obs_lon.length

    val obs_latMin = Obs_lat.min
    val obs_latMax = Obs_lat.max

    val obs_lonMin = Obs_lon.min
    val obs_lonMax = Obs_lon.max

    val time: Array[Float] = timeV.dataFloat()
    val timeIndex: Int = ArrayOperation.HashMapSearch(time, obstime)

    val heightIndex: Int = {
      if (heightLen != 1) {
        val msg = "数据源高度层数=" + heightLen + ";取0层高度"
        val e: Exception = new Exception(msg)
        e.printStackTrace()
      }
      0
    }


    if (Constant.PRE.equals(fcType)) {
      for (k <- 0 to lat.length - 1) {
        for (l <- 0 to lon.length - 1) {
          if (obs_latMin <= lat(k) && lat(k) <= obs_latMax && obs_lonMin <= lon(l) && lon(l) <= obs_lonMax) {
            val Klat: Int = ArrayOperation.nearIndex_sequence(Obs_lat, lat(k))
            val Llon: Int = ArrayOperation.nearIndex_sequence(Obs_lon, lon(l))

            val obs_index = IndexOperation.getIndex3_tlatlon(0, Klat, Llon, Obs_latLen, Obs_lonLen)
            val obsvalue = dataV_Obs.dataFloat(obs_index)

            if (math.abs(obsvalue - ObsRule.defaultValue) > 0.1d) {
              val fc_obs_index = IndexOperation.getIndex4_thlatlon(timeIndex, heightIndex, k, l, heightLen, latLen, lonLen)
              val fc_obs_value = dataV.dataFloat(fc_obs_index)
              if (obsvalue > ObsRule.PRECriticalValue && fc_obs_value == 0) {
                for (i <- (timeIndex to (timeIndex + ObsRule.PREReviseTimes - 1))) {
                  val index = IndexOperation.getIndex4_thlatlon(i, heightIndex, k, l, heightLen, latLen, lonLen)
                  dataV.setdata(index, ObsRule.PREStableValue)
                }
              } else if (obsvalue == 0 && fc_obs_value != 0) {
                for (i <- (timeIndex to (timeIndex + ObsRule.PREReviseTimes - 1))) {
                  val index = IndexOperation.getIndex4_thlatlon(i, heightIndex, k, l, heightLen, latLen, lonLen)
                  dataV.setdata(index, 0)
                }
              }
            }
          }
        }
      }
    } else if (Constant.TEM.equals(fcType)) {
      for (k <- 0 to lat.length - 1) {
        for (l <- 0 to lon.length - 1) {

          if (obs_latMin <= lat(k) && lat(k) <= obs_latMax && obs_lonMin <= lon(l) && lon(l) <= obs_lonMax) {
            val Klat: Int = ArrayOperation.nearIndex_sequence(Obs_lat, lat(k))
            val Llon: Int = ArrayOperation.nearIndex_sequence(Obs_lon, lon(l))

            val obs_index = IndexOperation.getIndex3_tlatlon(0, Klat, Llon, Obs_latLen, Obs_lonLen)
            val obs_value = dataV_Obs.dataFloat(obs_index)

            if (math.abs(obs_value - ObsRule.defaultValue) > 0.1d) {

              val fc_obs_index = IndexOperation.getIndex4_thlatlon(timeIndex, heightIndex, k, l, heightLen, latLen, lonLen)
              //fc_value_obs预报点在实况时刻的值
              val fc_obs_value = dataV.dataFloat(fc_obs_index)
              val dObsValue = fc_obs_value - obs_value
              val absdObsValue = math.abs(dObsValue)

              if ((ObsRule.TEMCriticalValueMin < absdObsValue) && (absdObsValue < ObsRule.TEMCriticalValueMax)) {
                for (i <- (0 to (ObsRule.TEMReviseTimes - 1))) {
                  val index = IndexOperation.getIndex4_thlatlon(i + timeIndex, heightIndex, k, l, heightLen, latLen, lonLen)
                  val fcvalue = dataV.dataFloat(index)
                  val obsvalue = fcvalue - (dObsValue - i * ObsRule.TEMDecay(dObsValue))
                  dataV.setdata(index, obsvalue)
                }
              }
            }
          }
        }
      }
    }
    dataV
  }

  def main(args: Array[String]): Unit = {
    val appId = ContextUtil.getApplicationID
    val obsFile = "/D:/Data/obs/WTRGO_10m_0_0_18.160_73.446_1_0_0.01_0.01_0_0_53.561_135.099_201804181430.nc"
    val obsdate: Date = DateFormatUtil.YYYYMMDDHHMMSS0("201804181430")
    val fcTypes = Array("TEM", "PRE")
    val obsdatas = ReadObsCW.ReadObs(obsFile, fcTypes)
    val PRE1m = obsdatas.apply("PRE10m").dataDouble()
    val TEM = obsdatas.apply("TEM").dataDouble()
    println()
  }
}
