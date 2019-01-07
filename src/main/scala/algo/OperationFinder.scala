package algo

import operations._
import structure.{ConjunctionConstraint, PsddDecision, PsddElement}
import util.Util

abstract class OperationFinder[O <: PsddOperation, I <: Object](mgr: PsddManager, operationCompletionType: OperationCompletionType, scorer: OperationScorer, parameterCalculator: ParameterCalculator, root: PsddDecision) {

  def find(input: I): (O, Double)

}


class SplitOperationFinder(mgr: PsddManager, operationCompletionType: OperationCompletionType, scorer: OperationScorer, parameterCalculator: ParameterCalculator, root: PsddDecision, maxNbSplits: Int)
  extends OperationFinder[Split, (PsddDecision, PsddElement)](mgr, operationCompletionType, scorer, parameterCalculator, root) {

  override def find(input: (PsddDecision, PsddElement)): (Split, Double) = {
    val (node, splitElement) = input
    if (splitElement.data.train.isEmpty) return (null, Double.NegativeInfinity)

    val vars = node.vtree.left.vars.toSet
    if (vars.isEmpty) return (null, Double.NegativeInfinity)
    var splits = Set(ConjunctionConstraint(Map()))
    var best = (null: Split, Double.NegativeInfinity, Set.empty[Int], "")

    var nbSplitsToDo = maxNbSplits

    var stop = false
    while(! stop && nbSplitsToDo>0){
      nbSplitsToDo -= 1
      stop = true

      splits.foreach{split =>
        var localBestSplits = splits
        var localBest = best

        vars.diff(split.constraint.keySet).foreach { v =>
          // split an existing split on another variable to get two splits
          val newSplits = Set(ConjunctionConstraint(split.constraint + (v->true)), ConjunctionConstraint(split.constraint + (v->false)))

          // only add this splits if it doesn't add a "False" node
          if (newSplits.forall(splitFormula =>
            !splitFormula.isImpliedBy(splitElement.prime.formula)
              && splitFormula.isSatisfiableIn(splitElement.prime.formula))){
            val updatedSplits = newSplits ++ splits - split
            // calculate the score of adding set of splits
            val op = new Split(node, splitElement, updatedSplits.toArray, parameterCalculator, operationCompletionType, root, mgr)
            val res = op.update()
            val score = if (res.dSize == 0) Double.NegativeInfinity else scorer.score(res)
            val str = splitsToString(updatedSplits)
            if ((score>=localBest._2  && !Util.isEqual(score, localBest._2))|| (Util.isEqual(score, localBest._2) && str > localBest._4)){
              localBestSplits = updatedSplits
              localBest = (op, score, res.changedNodes, str)
            }
          }
        }

        if ((localBest._2 >=best._2 && !Util.isEqual(localBest._2 , best._2)) || (Util.isEqual(localBest._2 , best._2) && localBest._4  > best._4)){
          best = localBest
          splits = localBestSplits
          stop = false
        }
      }
    }
    (best._1, best._2)
  }

  def splitsToString(splits: Set[ConjunctionConstraint]): String = {
    splits.map(_.constraint.map{case (v,a)=>if (a) v else -v}.toArray.sorted.mkString("[",",","]")).toArray.sorted.mkString("Split(",",",")")
  }
}



class CloneOperationFinder(mgr: PsddManager, operationCompletionType: OperationCompletionType, scorer: OperationScorer, parameterCalculator: ParameterCalculator, root: PsddDecision, maxNbCloneParents: Int)
  extends OperationFinder[Clone, (PsddDecision, Array[(PsddDecision,PsddElement)])](mgr, operationCompletionType, scorer, parameterCalculator, root) {

  override def find(input: (PsddDecision, Array[(PsddDecision, PsddElement)])): (Clone, Double) = {
    val (node, parents) = input
    if (parents.length<=1 || node.deterministic || node.elements.forall(_.data.train.isEmpty)) (null, Double.NegativeInfinity)

    else {
      val maxSetSize = if (parents.length>=maxNbCloneParents) maxNbCloneParents else parents.length
      val res = (1 to maxSetSize).flatMap(setSize => parents.toSet.subsets(setSize)).map(parents =>
        Clone(node, Array(parents), parameterCalculator, operationCompletionType, root, mgr)).map { op =>
        val res = op.update()
        val score = if (res.dSize==0) Double.NegativeInfinity else scorer.score(res)
        val changedNodes = res.changedNodes
        val str = parentsToString(op.parents.head)
        (op, score, changedNodes,str)
      }.maxBy(a=>(a._2,a._4))
      (res._1, res._2)
    }
  }

  def parentsToString(parents: Set[(PsddDecision, PsddElement)]): String = {
    parents.map{case(p,e) => (p.index,e.prime.index)}.toArray.sorted.mkString("Clone(",",",")")
  }
}
