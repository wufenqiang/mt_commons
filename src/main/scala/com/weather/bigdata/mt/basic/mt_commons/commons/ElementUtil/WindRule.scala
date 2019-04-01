package com.weather.bigdata.mt.basic.mt_commons.commons.ElementUtil

import com.weather.bigdata.mt.basic.mt_commons.commons.Constant

private object WindRule {
  def WindUV2S(WindU: Double, WindV: Double): Double = {
    Math.sqrt(WindU * WindU + WindV * WindV)
  }
  def WindUV2D(WindU: Double, WindV: Double): Double = {
    if (WindU > 0 && WindV > 0){
      (Math.atan(WindU / WindV) * 180 / Math.PI)
    }else if (WindU > 0 && WindV < 0){
      (90 - Math.atan(WindU / WindV) * 180 / Math.PI)
    }else if (WindU < 0 && WindV < 0) {
      (180 + Math.atan(WindU / WindV) * 180 / Math.PI)
    }else if (WindU < 0 && WindV > 0) {
      (270 - Math.atan(WindU / WindV) * 180 / Math.PI)
    }else{
      if (WindU == 0 ) {
        if(WindV >=0){
          0
        }else{
          180
        }
      }else{
        if(WindU >0){
          90
        }else{
          270
        }
      }
    }
  }

  def WindS2SG(WindS: Double): Byte={
    if (0.0d<=WindS && WindS < 3.4d){
      0
    }else if (3.4d <= WindS && WindS < 8.0d) {
      1
    }else if (8.0d <= WindS && WindS < 10.8d) {
      2
    }else if (10.8d <= WindS && WindS < 13.9d) {
      3
    }else if (13.9d <= WindS && WindS < 17.2d) {
      4
    }else if (17.2d <= WindS && WindS < 20.8d) {
      5
    }else if (20.8d <= WindS && WindS < 24.5d) {
      6
    }else if (24.5d <= WindS && WindS < 28.5d) {
      7
    }else if (28.5d <= WindS && WindS < 32.7d) {
      8
    }else if (32.7d <= WindS) {
      9
    }else{
      Constant.byteDefault
    }
  }
  /*  def WindD2DG(WindD: Double): Byte = {
      var DGrade :Byte= -1
      if (22.5d <= WindD && WindD < 67.5d) {
        DGrade = 1
      }else if (67.5d <= WindD && WindD < 112.5d) {
        DGrade = 2
      }else if (112.5d <= WindD && WindD < 157.5d) {
        DGrade = 3
      }else if (157.5d <= WindD && WindD < 202.5d) {
        DGrade = 4
      }else if (202.5d <= WindD && WindD < 247.5d) {
        DGrade = 5
      }else if (247.5d <= WindD && WindD < 292.5d) {
        DGrade = 6
      }else if (292.5d <= WindD && WindD < 337.5d) {
        DGrade = 7
      }else if (337.5d <= WindD || ( 0d<=WindD  && WindD < 22.5d)) {
        DGrade = 8
      }
      DGrade
    }*/
  def WindD2DG(WindD: Double): Byte = {

    if (22.5d <= WindD && WindD < 67.5d) {
      5
    }else if (67.5d <= WindD && WindD < 112.5d) {
      6
    }else if (112.5d <= WindD && WindD < 157.5d) {
      7
    }else if (157.5d <= WindD && WindD < 202.5d) {
      8
    }else if (202.5d <= WindD && WindD < 247.5d) {
      1
    }else if (247.5d <= WindD && WindD < 292.5d) {
      2
    }else if (292.5d <= WindD && WindD < 337.5d) {
      3
    }else{
      //337.5d <= WindD || ( 0d<=WindD  && WindD < 22.5d)
      4
    }
  }

  def WindUV2SG(WindU: Double, WindV: Double): Byte ={
    this.WindS2SG(this.WindUV2S(WindU,WindV))
  }
  def WindUV2DG(WindU: Double, WindV: Double): Byte ={
    this.WindD2DG(this.WindUV2D(WindU,WindV))
  }

  def main(args:Array[String]): Unit ={
    val u= -3.858
    val v= 4.650
    val d=this.WindUV2D(u,v)
    println(d)
  }
}
