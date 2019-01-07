package structure

import sdd.Sdd


/**
  * This class stores a PSDD element which is  a triple of the prime, sub and parameter. Additionally it stores
  * the data that passes through this edge and the formula of the element.
  *
  * @param prime
  * @param sub
  * @param data
  * @param formula
  * @param theta
  */
case class PsddElement(prime: PsddNode, sub: PsddNode, var data: DataSets, formula: Sdd, var theta: Double) {


  override def hashCode = (prime,data).hashCode()

  def canEqual(other: Any): Boolean = other.isInstanceOf[PsddElement]

  override def equals(other: Any) = other match {
    case other: PsddElement => other.canEqual(this) && {
      this.prime == other.prime &&
      this.sub == other.sub &&
//      this.theta == other.theta &&
      this.data == other.data
    }
    case _ => false
  }

  override def toString: String = (prime.index, sub.index, data.train.size, data.valid.size, data.test.size, formula, theta).toString()
}
