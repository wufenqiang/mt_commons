package com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil

private object Algorithm2D {
  def doubleLine(Q00: Float, Q01: Float, Q10: Float, Q11: Float, x1: Float, x2: Float, y1: Float, y2: Float, x: Float, y: Float): Float = {
    Q00 * (x2 - x) * (y2 - y) / ((x2 - x1) * (y2 - y1)) + Q10 * (x - x1) * (y2 - y) / ((x2 - x1) * (y2 - y1)) + Q01 * (x2 - x) * (y - y1) / ((x2 - x1) * (y2 - y1)) + Q11 * (x - x1) * (y - y1) / ((x2 - x1) * (y2 - y1))
  }

  /*def doubleLine (Q11: Double, Q12: Double, Q21: Double, Q22: Double, x1: Double, x2: Double, y1: Double, y2: Double, x: Double, y: Double): Double = {
    Q11 * (x2 - x) * (y2 - y) / ((x2 - x1) * (y2 - y1)) + Q21 * (x - x1) * (y2 - y) / ((x2 - x1) * (y2 - y1)) + Q12 * (x2 - x) * (y - y1) / ((x2 - x1) * (y2 - y1)) + Q22 * (x - x1) * (y - y1) / ((x2 - x1) * (y2 - y1))
  }*/
}
