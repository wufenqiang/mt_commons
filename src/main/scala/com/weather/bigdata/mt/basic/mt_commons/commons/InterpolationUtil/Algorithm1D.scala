package com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil

private object Algorithm1D {

  def Line(x1: Float, x2: Float, y1: Float, y2: Float, x: Float): Float = ((y2 - y1) / (x2 - x1)) * (x - x1) + y1
  def Line (x1: Double, x2: Double, y1: Double, y2: Double, x: Double): Double = ((y2 - y1) / (x2 - x1)) * (x - x1) + y1

  def Line_Percent(x1: Float, x2: Float, y1: Float, y2: Float, x: Float): Float = {
    val min: Float = 0f
    val max: Float = 100f
    Line_Between(x1, x2, y1, y2, x, min, max)
  }
  def Line_Percent(x1: Double, x2: Double, y1: Double, y2: Double, x: Double): Double ={
    val min: Double=0d
    val max: Double=100d
    Line_Between(x1, x2, y1, y2, x, min, max)
  }

  def Line_Between(x1: Float, x2: Float, y1: Float, y2: Float, x: Float, min: Float, max: Float): Float = {
    var y = Line(x1, x2, y1, y2, x)
    if (y < min) {
      y = min
    } else if (y > max) {
      y = max
    }
    y
  }
  def Line_Between(x1: Double, x2: Double, y1: Double, y2: Double, x: Double, min: Double, max: Double): Double = {
    var y = Line(x1, x2, y1, y2, x)
    if (y < min){
      y=min
    }else if (y > max){
      y=max
    }
    y
  }

  def Line_Min(x1: Float, x2: Float, y1: Float, y2: Float, x: Float, min: Float): Float ={
    var y = Line(x1, x2, y1, y2, x)
    if (y < min) {
      y = min
    }
    y
  }
  def Line_Min(x1: Double, x2: Double, y1: Double, y2: Double, x: Double, min: Double): Double ={
    var y = Line(x1, x2, y1, y2, x)
    if (y < min) {
      y = min
    }
    y
  }


  def Difference(dx: Double, y2: Double): Double = y2 / dx
  def Difference(dx: Float, y2: Float): Float = y2 / dx

  def Ave(sum: Double, n: Int): Double = sum / n
  def Ave(sum: Float, n: Int): Float = sum / n

  def Sum(y0: Double, y1: Double): Double = y0 + y1

  def Min(y1: Double, y2: Double): Double ={
    math.min(y1,y2)
  }

  def Min(y1: Float, y2: Float): Float = {
    math.min(y1, y2)
  }

  def Max(y1: Double, y2: Double): Double ={
    math.max(y1,y2)
  }

  def Max(y1: Float, y2: Float): Float = {
    math.max(y1, y2)
  }

  def Fill(x: Array[Double], xIndex: Int): Double = x(xIndex)

  def Fill(x: Array[Float], xIndex: Int): Float = x(xIndex)
 /* def Lag(x: Array[Double], y: Array[Double], x0: Array[Double]): Array[Double] = {
    val y0 = new Array[Double](x0.length)
    for(i<- 0 to x0.length-1){
      y0(i)=Lag(x,y,x0(i))
    }
    y0
  }*/

  /*def Lag(x: Array[Double], y: Array[Double], x0: Double): Double = { //		y=ValidData(y);
    var y0 = 0
    for(ib <- 0 to x.length-1){
      var k:Double=1d
      for(ic <- 0 to x.length-1){
        if(ib!=ic){
          k*=(x0-x(ic))/(x(ib)-x(ic))
        }
      }
      k*=y(ib)
      y0+=k
    }
    y0
  }*/
}
