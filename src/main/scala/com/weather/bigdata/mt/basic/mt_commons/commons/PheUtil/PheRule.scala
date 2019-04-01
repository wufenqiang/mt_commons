package com.weather.bigdata.mt.basic.mt_commons.commons.PheUtil

object PheRule {

  private val rainlevel1 :Array[Double]= Array(2.5d, 8.0d, 16.0d, 40.0d, 80.0d)
  private val snowlevel1 :Array[Double]= Array(0.4d, 1.4d, 3.0d, 4.0d, 5.0d)
  private val rainlevel3 :Array[Double]= Array(3.0d, 10.0d, 20.0d, 50.0d, 100.0d)
  private val snowlevel3 :Array[Double]= Array(0.5d, 1.5d, 4.0d, 7.0d, 10.0d)
  private val rainlevel6 :Array[Double]= Array(4.0d, 13.0d, 25.0d, 60.0d, 120.0d)
  private val snowlevel6 :Array[Double]= Array(0.7d, 2.0d, 5.0d, 8.0d, 12.0d)
  private val rainlevel12 :Array[Double]= Array(10.0d, 25.0d, 50.0d, 100.0d, 250.0d)
  private val snowlevel12 :Array[Double]= Array(2.5d, 5.0d, 10.0d, 20.0d, 30.0d)

  //特殊天气现象(4:雷阵雨,18:雾,20:沙尘暴,29:浮尘,30:扬沙,31:强沙尘暴,53:霾)
  private val PheExcept:Array[Double]=Array(4d,18d,20d,29d,30d,31d,53d)


  //对v0,v3各种关键分级参数设置
  def Phe_level0 (timeStep:Double): (Array[Double],Array[Double]) ={
    val rainlevel :Array[Double]= {
      if(timeStep == 1.0d){
        rainlevel1
      }else if(timeStep == 3.0d){
        rainlevel3
      }else if(timeStep == 12.0d){
        rainlevel12
      }else{
        Array(10.0f, 25.0f, 50.0f, 100.0f, 250.0f)
      }
    }
    val snowlevel :Array[Double]= {
      if(timeStep == 1.0d){
        snowlevel1
      }else if(timeStep == 3.0d){
        snowlevel3
      }else if(timeStep == 12.0d){
        snowlevel12
      }else{
        Array(2.5f, 5.0f, 10.0f, 20.0f, 30.0f)
      }
    }
    (rainlevel,snowlevel)
  }

  //pre,pph,cloud对天气现象的正演算法v3
  def v3 (pre0: Double, pph0: Double, cloud0: Double, rainlevel: Array[Double], snowlevel: Array[Double]): Byte = {
    if(pre0==0){
      if(cloud0<20){
        0
      }else if(cloud0>80){
        2
      }else{
        1
      }
    }else{
      if (pph0 == 1) {
        if (pre0 <= snowlevel(0)) {
          14 //小雪
        } else if (snowlevel(0) < pre0 & pre0 <= snowlevel(1)) {
          15 //中雪
        } else if (snowlevel(1) < pre0 & pre0 <= snowlevel(2)) {
          16 //大雪
        } else {
          17 //大暴雪
        }
      } else if (pph0 == 2) {
        6
      } else if (pph0 == 3) {
        if(pre0 <= rainlevel(0)){
          7 //小雨
        }else if(rainlevel(0)<pre0 & pre0 <= rainlevel(1)){
          8 //中雨
        }else if(rainlevel(1)<pre0 & pre0 <= rainlevel(2)) {
          9 //大雨
        }else if(rainlevel(2)<pre0 & pre0 <= rainlevel(3)){
          10 //暴雨
        }else if(rainlevel(3)<pre0 & pre0 <= rainlevel(4)){
          11 //大暴雨
        }else{
          12 //特大暴雨
        }
      } else {
        //(pph0==4)
        19
      }
    }
  }

  //特殊天气现象不做天气现象重转——obs订正用
  def v1 (pre0: Double, tem0: Double, cloud0: Double, phe0: Byte, rainlevel: Array[Double], snowlevel: Array[Double]): Byte = {
    if (PheExcept.contains(phe0)) {
      phe0
    } else {
      this.v0(pre0, tem0, cloud0, rainlevel, snowlevel)
    }
  }

  //pre,tem,cloud对天气现象的正演算法v0
  def v0 (pre0: Double, tem0: Double, cloud0: Double, rainlevel: Array[Double], snowlevel: Array[Double]): Byte = {
    if(pre0==0){
      if(cloud0<20){
        0
      }else if(cloud0>80){
        2
      }else{
        1
      }
    }else{
      if (tem0 > 2) {
        if(pre0 <= rainlevel(0)){
          7 //小雨
        }else if(rainlevel(0)<pre0 & pre0 <= rainlevel(1)){
          8 //中雨
        }else if(rainlevel(1)<pre0 & pre0 <= rainlevel(2)) {
          9 //大雨
        }else if(rainlevel(2)<pre0 & pre0 <= rainlevel(3)){
          10 //暴雨
        }else if(rainlevel(3)<pre0 & pre0 <= rainlevel(4)){
          11 //大暴雨
        }else{
          12 //特大暴雨
        }
      } else if (tem0 < -2) {
        if (pre0 <= snowlevel(0)) {
          14 //小雪
        } else if (snowlevel(0) < pre0 & pre0 <= snowlevel(1)) {
          15 //中雪
        } else if (snowlevel(1) < pre0 & pre0 <= snowlevel(2)) {
          16 //大雪
        } else {
          17 //大暴雪
        }
      } else {
        6
      }
    }


  }

  //仅特殊天气现象做覆盖订正——ocf订正用
  def v2 (phe: Byte, phe_ocf: Byte): Byte = {
    if(PheExcept.contains(phe_ocf)){
      phe_ocf
    }else{
      phe
    }
  }

}
