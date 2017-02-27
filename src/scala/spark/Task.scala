package spark

import mesos._

trait Task[T] extends Serializable {
  def run: T
  def preferredLocations: Seq[String] = Nil
  def markStarted(offer: SlaveOffer) {}
}

class FunctionTask[T](body: () => T)
extends Task[T] {
  def run: T = body()
}
