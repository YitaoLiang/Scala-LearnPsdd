package structure

import operations.{PsddQueries, CloneSpecification}
import sdd.Sdd
import util.Log

import scala.collection.mutable

/**
  * A PSDD node
  *
  * @param index
  * @param vtree
  * @param formula
  */
abstract class PsddNode(val index: Int, val vtree: VtreeNode, val formula: Sdd) {

  var flag = false
  val deterministic: Boolean
  var baggage: Any = null
  var cloneSpec: CloneSpecification = null

  override def hashCode = index.hashCode

  def canEqual(other: Any): Boolean = other.isInstanceOf[PsddNode]

  override def equals(that: Any) = that match {
    case that: PsddNode => that.canEqual(this) && this.index == that.index
    case _ => false
  }

  def elements = Set[PsddElement]()

  val mc: Double
}

/**
  * A PSDD decision node. It contains elements that link to its children.
  * @param index
  * @param vtree
  * @param elementSet
  * @param formula
  */
case class PsddDecision(override val index: Int, override val vtree: VtreeInternal, var elementSet: Set[PsddElement], override val formula: Sdd) extends PsddNode(index, vtree, formula) {
  def noTrainData: Boolean = elements.forall(_.data.train.isEmpty)

  assert (elementSet.nonEmpty)
  override val deterministic = elements.size==1 && elements.head.prime.deterministic && elements.head.sub.deterministic

  def data: DataSets = {
    if (elements.isEmpty) println(this)
    elements.map(el=>el.data).reduce(_.union(_))
  }

  def trainData: Data = {
    if (elements.isEmpty) println(this)
    elements.map(el=>el.data.train).reduce(_.union(_))
  }

  def getElement(prime:PsddNode, sub: PsddNode) = elements.find(el => el.prime == prime && el.sub == sub)

  override def elements = elementSet

  val mc = elements.toList.map{case PsddElement(prime,sub,_,_,_) => Log.multiply(prime.mc,sub.mc)}.reduce(Log.add)

  override def toString:String = "PsddDecision"+(index, vtree.index, elementSet.map(el=>(el.prime.index, el.sub.index)),formula.getVisualization)

  require(PsddQueries.isValidDecisionNode(this), "The created decision node is not valid!")
}


/**
  * A PSDD literal. This is a PSDD terminal node that has a literal as its formula (e.g. not X)
  * @param index
  * @param vtree
  * @param literal
  * @param formula
  */
case class PsddLiteral(override val index: Int, override val vtree: VtreeVar, literal: Int, override val formula: Sdd) extends PsddNode(index, vtree, formula){
  override val deterministic = true
  val v = math.abs(literal)
  val pos = literal>0
  val mc = Log.one
}


/**
  * A PSDD true node. This is a PSDD terminal node that has "true" as its formula and does not contain any variables.
  * @param index
  * @param formula
  */
case class PsddTrue(override val index: Int, override val formula: Sdd) extends PsddNode(index,VtreeTerminal, formula){
  override val deterministic = true
  val mc = Log.one
}