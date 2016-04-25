import scala.util.control.Breaks

def isdigitString(x:String) = {
  var result = true
  val loop = new Breaks
  loop.breakable {
    for (i <- x) {
      if(!i.isDigit){
        result = false
        loop.break
      }
    }
  }
  result
}
isdigitString("123")

