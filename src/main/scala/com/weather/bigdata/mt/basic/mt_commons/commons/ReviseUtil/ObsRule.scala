package com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil

private object ObsRule {
  val defaultValue:Double= -99.99d
//  private val defaultValues:Array[Double]=Array( -99.99d )

  /*def ReviseTimes(FcType: String): Int = {
    if (ConstantUtil.TEM.equals(FcType)) {
      ObsRule.TEMReviseTimes
    } else if (ConstantUtil.PRE.equals(FcType)) {
      ObsRule.PREReviseTimes
    }else {
      val e:Exception=new Exception("ReviseTimes 不包含类型" + FcType)
      e.printStackTrace()
      -1
    }
  }*/


  //当abs(Fc-obs)>TEMCriticalValue,TEMDecay温度衰减系数
  val TEMCriticalValueMin :Double = 1.0d
  val TEMCriticalValueMax :Double = 10.0d

  val TEMReviseTimes: Int = 12
  //  val TEMDecay: Double = 1.0d/this.TEMReviseTimes.toDouble

  def TEMDecay (dObsValue: Double): Double = dObsValue / this.TEMReviseTimes.toDouble

  def main(args:Array[String]): Unit ={

  }

  /*def setTEM(TEMCriticalValue: Double, TEMDecay: Double, TEMReviseTimes: Int): Unit = {
    ObsRule.TEMCriticalValue = TEMCriticalValue
    ObsRule.TEMDecay = TEMDecay
    if (TEMReviseTimes > 0) {
      ObsRule.TEMReviseTimes = TEMReviseTimes
    } else {
      val msg = "设定订正时间层数=" + TEMReviseTimes + "<=0,未修改缺省值(TEMReviseTimes=24)"
      println(msg)
    }
  }*/

  /*def TEMObsRule(source: Double, dObsValue: Double, i: Int): Double = {

    /*val dObsValue :Double = Math.abs(source - obs)
    if (ObsRule.TEMCriticalValueMin < dObsValue && dObsValue<ObsRule.TEMCriticalValueMax){
      //        source - dObsValue*(1 - i * ObsRule.TEMDecay)
      obs + i * ObsRule.TEMDecay
    }else{
      source
    }*/
    source - (dObsValue - i * ObsRule.TEMDecay)
  }*/


  //当(obs>PRECriticalValue && Fc==0)则Fc=PREStableValue,当(obs==0 && Fc!=0)则Fc=0;
  val PRECriticalValue :Double = 0.0d
  //  protected var PRECriticalValue :Double = 16.0d
  val PREStableValue :Double = 2.5d
  val PREReviseTimes :Int = 2

  /*private def setPRE(PRECriticalValue: Double, PREStableValue: Double, PREReviseTimes: Int): Unit = {
    ObsRule.PRECriticalValue = PRECriticalValue
    ObsRule.PREStableValue = PREStableValue
    if (PREReviseTimes > 0) {
      ObsRule.PREReviseTimes = PREReviseTimes
    } else {
      val msg = "设定订正时间层数=" + PREReviseTimes + "<=0,未修改缺省值(PREReviseTimes=2)"
      println(msg)
    }
  }*/

  /*def PREObsRule(source: Double, obs: Double): Double = {
    if(math.abs(obs-this.defaultValue)>0.1d) {
      if (obs > ObsRule.PRECriticalValue && source == 0) {
        ObsRule.PREStableValue
      } else if (obs == 0 && source != 0) {
        0
      } else {
        source
      }
    }else{
      source
    }
  }*/

  //
  val WEATHERReviseTimes = 2
  /*def setWEATHER(WEATHERReviseTimes: Int): Unit = {
    if (WEATHERReviseTimes > 0) ObsRule.WEATHERReviseTimes = WEATHERReviseTimes
    else {
      val msg = "设定订正时间层数=" + WEATHERReviseTimes + "<=0,未修改缺省值(WEATHERReviseTimes=2)"
      println(msg)
    }
  }*/
}
