package util

import operations.PsddQueries
import sdd.Sdd
import structure._

import scala.collection.mutable

/**
  * Class to compare PSDDs
  *
  * This can be used for debugging
 *
 */
object PsddComparator {

  //assert same sdd manager
  def psddEqual(psdd1: PsddNode, psdd2: PsddNode, checkParameters: Boolean, checkData: Boolean): Boolean = {
//    println()
    assert(psdd1.formula.getManager==psdd2.formula.getManager)
    if (psdd1.formula!=psdd2.formula) return false
    PsddQueries.setContextsAsBaggage(psdd1)
    PsddQueries.setContextsAsBaggage(psdd2)
    val res = psddEqual(psdd1, psdd2, checkParameters, checkData, mutable.Map.empty[Int,Int])
    PsddQueries.nodes(psdd1).foreach(_.baggage=null)
    PsddQueries.nodes(psdd2).foreach(_.baggage=null)
    res
  }

  private def psddEqual(psdd1: PsddNode, psdd2: PsddNode, checkParameters: Boolean, checkData: Boolean, equalMap: mutable.Map[Int,Int]): Boolean = {
//    println(psdd1.index, psdd2.index, psdd1.formula.getVisualization, psdd2.formula.getVisualization, psdd1.baggage.asInstanceOf[Sdd].getVisualization, psdd2.baggage.asInstanceOf[Sdd].getVisualization)
    if (psdd1.elements.size!=psdd2.elements.size) {println("different elements",psdd1.index,psdd2.index);return false}
    if (psdd1.formula!=psdd2.formula)  {println("different formulas",psdd1.vtree.index,psdd1.index,psdd2.index);println(psdd1.formula.getVisualization);println(psdd2.formula.getVisualization);return false}
    if (psdd1.baggage!=psdd2.baggage)  {println("different contexts",psdd1.vtree.index,psdd1.index,psdd2.index);println("f1",psdd1.formula.getVisualization);println("f2",psdd2.formula.getVisualization);println("c1",psdd1.baggage.asInstanceOf[Sdd].getVisualization);println("c2",psdd2.baggage.asInstanceOf[Sdd].getVisualization);return false}
    if (!psdd1.elements.forall{ el1 =>
      val el2Option = psdd2.elements.find(_.formula==el1.formula)
      if (el2Option.isDefined){
        val el2 = el2Option.get
        if (checkParameters && !Util.isEqual(el1.theta, el2.theta))  {println("problematic node:",psdd1.index, psdd2.index, el1.theta, el2.theta); false}
        else if (checkData && el1.data!=el2.data) {println("problematic node:",psdd1.index, psdd2.index, el1.data.train.size, el2.data.train.size, el1.data.valid.size, el2.data.valid.size, el1.data.test.size, el2.data.test.size); false}
        else {
          (
            if (equalMap.contains(el1.prime.index)) equalMap(el1.prime.index)==el2.prime.index
            else psddEqual(el1.prime, el2.prime, checkParameters, checkData, equalMap)
            ) && (
            if (equalMap.contains(el1.sub.index)) equalMap(el1.sub.index)==el2.sub.index
            else psddEqual(el1.sub, el2.sub, checkParameters, checkData, equalMap)
            )
        }
      }
      else true
    }) false
    else true
  }

  def vtreeBottomUp(vtree: VtreeNode): Array[VtreeNode] = {
    var vtreeList = Array(vtree)
    var i = 0
    while(i<vtreeList.length){
      vtree match {
        case v: VtreeInternalVar =>
          vtreeList :+= v.left
        case v: VtreeInternal =>
          vtreeList :+= v.left
          vtreeList :+= v.right
        case _ =>

      }
      i+=1
    }
    vtreeList.reverse
  }

  private def vtreeEqual(vtree1: VtreeNode, vtree2: VtreeNode, vtreeList1: Array[VtreeNode], vtreeList2: Array[VtreeNode]): Boolean = {
    if (vtreeList1.length!=vtreeList2.length) return false
    if (vtreeList1.zip(vtreeList2).exists{case (a,b) => a.index!=b.index}) return false
    if (vtree1.vars.zip(vtree2.vars).exists{case (a,b)=>a!=b})return false
    true
  }

  def vtreeEqual(vtree1: VtreeNode, vtree2: VtreeNode): Boolean = vtreeEqual(vtree1, vtree2, vtreeBottomUp(vtree1), vtreeBottomUp(vtree2))

}
