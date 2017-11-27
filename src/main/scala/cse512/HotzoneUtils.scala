package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var point = new Array[Double](2)
    var rectangle = new Array[Double](4)
    var i=0
    for(x <- pointString.split(",") ) {
      point(i) = x.toDouble; i+=1;
    }
    i=0;
    for(x <- queryRectangle.split(",") ) {
      rectangle(i) = x.toDouble; i+=1;
    }
    i=0
    var inside = true
    for(pt <- point) {
      val valid = (rectangle(i)<=pt && pt<=rectangle(i+2)) || (rectangle(i)<=pt && pt<=rectangle(i+2))
      inside= inside && valid
      i+=1
    }
    return inside
  }

  // YOU NEED TO CHANGE THIS PART

}
