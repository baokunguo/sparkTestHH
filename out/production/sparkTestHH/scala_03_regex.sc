import scala.util.matching.Regex

val pattern = new Regex("(S|s)cala")
val test_Str = "Scala is scalable and cool"
for (i <- pattern.findAllIn(test_Str))
  println(i + " asdf ")

val test_Double = "asdf"
val doublePattern = new Regex("\\d.*")
for (i <- doublePattern.findAllIn(test_Double))
  println("asdf\t" + i + "\n")


