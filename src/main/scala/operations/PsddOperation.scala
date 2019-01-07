package operations

import algo.ParameterCalculator
import structure.{Constraint, PsddElement, PsddDecision}

/**
  * This class represents an operation that can be done on a PSDD (split, clone, maybe merge in the future).
  *
  * The operation can be executed or simulated
  * The class stores the log likelihood gain, increase in number of edges and the set of nodes that are changed by this
  * operation.
  *
 */
abstract class PsddOperation(parameterCalculator: ParameterCalculator, completionType: OperationCompletionType, mgr: PsddManager, root:PsddDecision) {

  def execute(): ExecutionResult

  protected def simulate(): SimulationResult

  def update(): SimulationResult = {
    val res = simulate()
    dll = res.dll
    dSize = res.dSize
    changedNodes = res.changedNodes
    res
  }

  var dll: Double = 0.0
  var dSize: Int = 0
  var changedNodes: Set[Int] = Set.empty[Int]

}

/**
  * The result of simulating or executing an operation
  */
trait Result{
  def dll: Double
  def dSize: Int
  def changedNodes: Set[Int]
}

/**
  *
  * @param dll
  * @param dSize
  * @param changedNodes
  */
case class SimulationResult(dll: Double, dSize: Int, changedNodes: Set[Int]) extends Result

/**
  *
  * @param dll
  * @param validDll
  * @param testDll
  * @param dSize
  * @param changedNodes
  * @param newNodes
  */
case class ExecutionResult(dll: Double, validDll: Double, testDll: Double, dSize: Int, changedNodes: Set[Int], newNodes: Set[Int]) extends Result

case class Split(splitNode: PsddDecision, splitElement: PsddElement, splitFormulas: Array[Constraint], parameterCalculator: ParameterCalculator, completionType: OperationCompletionType, root: PsddDecision, mgr: PsddManager) extends PsddOperation(parameterCalculator, completionType, mgr, root) {
  val subVars = splitNode.vtree.right.vars
  assert(splitFormulas.forall(!_.vars.exists(subVars.contains)),"split on sub")

  override def simulate(): SimulationResult = mgr.simulateSplit(splitNode, splitElement, parameterCalculator, splitFormulas, completionType)

  override def execute(): ExecutionResult = mgr.executeSplit(splitNode, splitElement, parameterCalculator, splitFormulas, completionType, root)

  override def toString: String = "Split("+splitNode.index+","+splitNode.vtree.index+","+splitElement.prime.index+","+splitFormulas.mkString("[",",","]")+","+completionType+")"

  override def equals(other: Any): Boolean = other match{
    case other: Split =>
      this.splitNode == other.splitNode &&
      this.splitElement.prime == other.splitElement.prime &&
      this.splitFormulas.toSet==other.splitFormulas.toSet

    case _ => false
  }

  override def hashCode: Int = (splitNode.index, splitElement.prime.index, splitFormulas).hashCode()
}

case class Clone(node: PsddDecision, parents: Array[Set[(PsddDecision, PsddElement)]], parameterCalculator: ParameterCalculator, completionType: OperationCompletionType, root: PsddDecision, mgr: PsddManager) extends PsddOperation(parameterCalculator, completionType, mgr, root) {

  override def simulate(): SimulationResult = mgr.simulateClone(node, parameterCalculator, parents, completionType)

  override def execute(): ExecutionResult = mgr.executeClone(node, parameterCalculator, parents, completionType, root)

  override def toString: String = "Clone("+node.index+","+node.vtree.index+","+parents.map( _.map(e=>e._1.index+":"+e._2.prime.index).mkString("{",",","}")).mkString("[",",","]")+","+completionType+")"

  override def equals(other: Any): Boolean = other match{
    case other: Clone =>
      this.node == other.node &&
        this.parents.toSet==other.parents.toSet

    case _ => false
  }

  override def hashCode: Int = (node.index, parents.map(_.map(a=>(a._1.index, a._2.prime.index))).toSet).hashCode()
}