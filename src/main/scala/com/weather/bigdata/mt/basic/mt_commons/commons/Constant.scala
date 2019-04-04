package com.weather.bigdata.mt.basic.mt_commons.commons

object Constant {
  val doubleDefault: Double = Double.NaN
  val floatDefault: Float = Float.NaN
  val byteDefault: Byte = Byte.MinValue
  val intDefault: Int = Int.MinValue
  val shortDefault: Short = Short.MinValue
  val longDefault: Long = Long.MinValue
  val chartDefault: Char = Char.MinValue


  val Default:Double= -9999d
  val PREDefault:Double= -9999d
  val TEMDefault:Double= -99d
  val CLOUDDefault:Double=9999d
  val WINDDefault:Double= -9d
  val WINSDefault:Double= -9d
  val WEATHERDefault:Double= -9d
  val PRSDefault:Double= -999d

  val LineKey:String="Line"
  val Line_PercentKey:String="Line_Percent"
  val Line_Between0Key:String="Line_Between_300_1080"
  val Line_Between1Key:String="Line_Between_0"
  val DifferenceKey:String="Difference"
  val AveKey:String="Ave"
  val SumKey:String="Sum"
  val MinKey:String="Min"
  val MaxKey:String="Max"
  val FillKey:String="Fill"
  val WhichFirstKey:String="WhichFirst"
  val WhichSecondKey:String="WhichSecond"
  val WhichMaxIndexKey:String="WhichMaxIndex"

  val PRE: String = "PRE"
  val TEM: String = "TEM"
  val CLOUD: String = "CLOUD"
  val RHU: String = "RHU"
  val WIND: String = "WIND"
  val PRS: String = "PRS"
  val WEATHER: String = "WEATHER"
  val VIS: String = "VIS"
  val PPH: String = "PPH"
  val WINUV: String = "WINUV"

  val PREPathKey: String = "Gr2b_PRE"
  val TEMPathKey: String = "Gr2b_TEM"
  val CLOUDPathKey: String = "Gr2b_CLOUD"
  val RHUPathKey: String = "Gr2b_RHU"
  val WINDPathKey: String = "Gr2b_WIND"
  val PRSPathKey: String = "Gr2b_PRS"
  val VISPathKey: String = "Gr2b_VIS"

  val PPHPathKey: String = "Gr2b_PPH"

  val Ocf1hPathKey: String = "Ocf_1h"
  val Ocf12hPathKey: String = "Ocf_12h"

  val reduceLatLon_CLOUDPathKey: String = "reduceLatLon_CLOUD"
  val reduceLatLon_PREPathKey: String = "reduceLatLon_PRE"
  val reduceLatLon_PRSPathKey: String = "reduceLatLon_PRS"
  val reduceLatLon_RHUPathKey: String = "reduceLatLon_RHU"
  val reduceLatLon_TEMPathKey: String = "reduceLatLon_TEM"
  val reduceLatLon_VISPathKey: String = "reduceLatLon_VIS"
  val reduceLatLon_WEATHERPathKey: String = "reduceLatLon_WEATHER"
  val reduceLatLon_WINDPathKey: String = "reduceLatLon_WIND"
  val reduceLatLon_WINUVPathKey: String = "reduceLatLon_WINUV"

  val reduceLatLon: String = "reduceLatLon"


  val OrdinaryPathKeys: Array[String] = Array(PREPathKey, TEMPathKey, CLOUDPathKey, RHUPathKey, WINDPathKey, PRSPathKey, VISPathKey)
  val SpecialPathKeys: Array[String] = Array(PPHPathKey)
  val OcfPathKeys: Array[String] = Array(Ocf1hPathKey, Ocf12hPathKey)
  val ObsPathKeys: Array[String] = Array("Obs")
  val OtherPathKeys: Array[String] = Array("Other")

  val DataTypes: Array[String] = this.OrdinaryPathKeys.union(this.SpecialPathKeys).union(this.OcfPathKeys).union(this.ObsPathKeys).union(this.OtherPathKeys)

  val OrdinaryFcTypes: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.VIS)

  val Phe0PathKeys: Array[String] = Array(this.PREPathKey, this.TEMPathKey, this.CLOUDPathKey)
  val Phe1PathKeys: Array[String] = Array(this.PREPathKey, this.CLOUDPathKey, this.PPHPathKey)
  val Phe0Types: Array[String] = Array(this.PRE, this.TEM, this.CLOUD)
  val Phe1Types: Array[String] = Array(this.PRE, this.TEM, this.PPH)
  val noPhePathKeys: Array[String] = Array(this.WINDPathKey, this.RHUPathKey, this.PRSPathKey)

  val FcTypes: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.WEATHER, this.VIS)
  val FcTypes_obs: Array[String] = Array(this.PRE, this.TEM, this.WEATHER)
  val FcTypes_ocf: Array[String] = Array(this.PRE, this.TEM, this.WEATHER)
  val FcTypes_inputKV: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.WEATHER)
  val FcTypes_reduceLatLon: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.WEATHER, this.VIS, this.WINUV)
  //  val FcTypes_changeRefTime: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.WEATHER, this.VIS,this.WINUV)
  val FcTypes_changeRefTime: Array[String] = Array(this.PRE, this.TEM, this.CLOUD, this.RHU, this.WIND, this.PRS, this.WEATHER, this.VIS, this.WINUV)

  val TimeSteps: Array[Double] = Array(1.0d, 12.0d)

  val SourceDateTypeKey: String = "SourceDateType"
  val Gr2bKey: String = "Gr2b"
  val ObsKey: String = "Obs"
  val OcfKey: String = "Ocf"
  val Ocf1hKey: String = Ocf1hPathKey
  val Ocf12hKey: String = Ocf12hPathKey

  val ChangeReftimeKey: String = "ChangeReftime"
  val InputKVKey: String = "InputKV"
  val OtherKey: String = "Other"

  val KVStore_ChangePerson = "default"
  val Obs_ChangePerson = "default"
  val Ocf_ChangePerson = "default"
  val Gr2b_ChangePerson = "default"
  val fcType_ChangePerson = "default"

  val basicProcessKey = "basicProcess"

  val timeStepsKey: String = "timeSteps"
  val splitFileKey: String = "splitFile"

  val regionKey = "region0"
}
