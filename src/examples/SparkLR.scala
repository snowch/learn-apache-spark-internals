import java.util.Random
import scala.math.exp
import Vector._
import spark._

object SparkLR {
  val N = 10000  // Number of data points
  val D = 10   // Numer of dimensions
  val R = 0.7  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if(i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint _)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkLR <host> [<slices>]")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "SparkLR")
    val numSlices = if (args.length > 1) args(1).toInt else 2
    val data = generateData

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = sc.accumulator(Vector.zeros(D))
      for (p <- sc.parallelize(data, numSlices)) {
        val scale = (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y
        gradient +=  scale * p.x
      }
      w -= gradient.value
    }

    println("Final w: " + w)
  }
}
