package algo

import main.{Debug, Output}
import operations._
import structure.{PsddDecision, PsddElement}
import util.Util

import scala.collection.mutable



/**
  * This class implements an operation queue that exists of several specific operation queues.
  * @param nodeSelectionCriterion The criterion for nodes to consider
  * @param addNewNodes True if operations on new nodes resulting from previous operations are added to the queue
  * @param removeOperationAfterExecution True if other operations on a node should be removed after executing an operation on that node
  * @param queues queues for specific operations
  */
class OperationQueue(
                      nodeSelectionCriterion: PsddDecision=>Boolean,
                      addNewNodes: Boolean,
                      removeOperationAfterExecution: Boolean,
                      queues: Array[SpecificOperationQueue[_ <: PsddOperation, _<: Object]]
                    ) {

  var last: PsddOperation = _

  Debug.addWriter("ops", "OperationQueue.txt", 2)

  def hasNext: Boolean = queues.exists(_.hasNext)

  def next: PsddOperation = {
    last = queues.filter(_.hasNext).map(_.next).maxBy(_._2)._1
    Debug.writeln("ops", 2, "\nIt " + Output.getCounter("cycle") + "-" + Output.getCounter("it") + ": " + last + "\n")
    last
  }

  def initialize(root: PsddDecision, psddManager: PsddManager): Unit = {
    Debug.writeln("ops", 2, "\n Initialize queue\n")
    queues.foreach(_.initialize(root, psddManager, nodeSelectionCriterion, addNewNodes))
  }

  def update(res: ExecutionResult): Unit = {
    queues.foreach(_.update(last, res))

    if (removeOperationAfterExecution) {
      val nodesOfOp = last match {
        case op:Split => Set (op.splitNode.index)
        case op:Clone => Set (op.node.index)
        case _ => Set[Int]()
      }
      queues.foreach(_.removeOperationsWith(nodesOfOp))
    }
  }

}


/**
  * This abstract class represents the queue for a specific type of operation
  * @param operationFinder class that finds the best operation for a U
  * @tparam F the type of operation in the queue
  * @tparam U The input needed to find an operation
  */
abstract class SpecificOperationQueue[F <: PsddOperation, U <: Object]( operationFinder: OperationFinder[F,U] ) {

  protected var operationQueue: mutable.TreeSet[(F,Double)] = _

  protected var root: PsddDecision = _
  protected var mgr: PsddManager = _

  protected var nodeSelectionCriterion: PsddDecision=>Boolean = _
  var addNewNodes: Boolean = true

  def removeOperationsWith(nodesToRemove: Set[Int]): Unit

  def initialize(root: PsddDecision, psddManager: PsddManager, nodeSelectionCriterion: PsddDecision=>Boolean, addNewNodes: Boolean): Unit = {
    this.root = root
    this.mgr = mgr
    this.nodeSelectionCriterion = nodeSelectionCriterion
    this.addNewNodes = addNewNodes

    // implement operationQueue initialization in subclasses
  }

  def hasNext: Boolean  =operationQueue.headOption match {
    case None => false
    case Some(op) => op._1!=null
  }

  def next: (F, Double) = operationQueue.head

  def update(lastOp: PsddOperation, res: ExecutionResult): Unit = {
    removeOperationsWithUnreachableNodes()
  }

  protected def usesNonFlaggedNode(op: F): Boolean



  protected def removeOperationsWithUnreachableNodes() = {
    val reachableNodes = PsddQueries.decisionNodes(root)
    reachableNodes.foreach(_.flag = true)
    operationQueue.filter(op => usesNonFlaggedNode(op._1)).foreach(operationQueue -= _ )
    reachableNodes.foreach(_.flag = false)
  }
}


/**
  * This class represents the queue of clones, with one clone per node
  * @param operationFinder class that finds the best operation for a certain node
  */
class CloneOperationQueue(operationFinder: CloneOperationFinder)
  extends SpecificOperationQueue[Clone, (PsddDecision, Array[(PsddDecision,PsddElement)])](operationFinder) {

  override def initialize(root: PsddDecision, psddManager: PsddManager, nodeSelectionCriterion: PsddDecision=>Boolean, addNewNodes: Boolean): Unit = {
    super.initialize(root, psddManager, nodeSelectionCriterion, addNewNodes)

    operationQueue = new mutable.TreeSet[(Clone,Double)]()(Ordering.by(op => (-op._2, op._1.node.index)))
    addOperations()
  }

  override def update(lastOp: PsddOperation, res: ExecutionResult): Unit = {
    super.update(lastOp, res)


    //////// Find all the nodes for which the operation should be (re)calculated ////////
    //
    // A clone should be updated if:
    // * One of its nodes is affected by the previous operation
    // * The parent of one of its nodes is affected by the previous operation
    //
    // If we see the children of the affected nodes also as affected nodes, then it is only the first test
    //
    // Actually over conservative.... This is more efficient:
    //
    // Recalculate:
    // * any operation that has a changed node that was changed by the previous operation
    // * The operations on the children of the nodes that were changed by the previous operation.


//    // here we only look at children of changed nodes, not new nodes. For a node to have a new parent, it also needs a changed parent or is new itself, so no need to consider it here.
//    val affectedNodes = res.changedNodes ++ PsddQueries.decisionNodes(root).filter(node => nodeSelectionCriterion(node) &&  res.changedNodes.contains(node.index)).flatMap(_.elements.flatMap(el=>Set(el.prime.index, el.sub.index))).toSet
//
//    var nodesToUpdate = operationQueue.withFilter(_._1.changedNodes.exists(affectedNodes.contains)).map(_._1.node.index)

    // add any operation that has a changed node that was changed by the previous operation
    println("changedNodes: "+res.changedNodes)
    operationQueue.withFilter(_._1!=null).foreach(op => println(op._1.node.index+": "+op._1.changedNodes))
    var nodesToUpdate = operationQueue.withFilter(op => op._1!=null && op._1.changedNodes.exists(res.changedNodes.contains)).map(_._1.node.index).toSet
    println("nodesToUpdate: "+nodesToUpdate)
    // add children of the changed nodes
    val possibleParents = res.changedNodes ++ res.newNodes
    val children = PsddQueries.decisionNodes(root).withFilter(node => possibleParents.contains(node.index)).flatMap(node => node.elements.flatMap(el => Set(el.prime.index, el.sub.index)))
    nodesToUpdate ++= children
    println("children: "+children)
    // add the new nodes to the operations if this is desired
    if (addNewNodes) nodesToUpdate ++= res.newNodes
    println("new nodes: "+res.newNodes)


    ////////  update the operations of the nodes that should be updated ////////

    updateOperationsWith(nodesToUpdate)

    //for debugging:
    assert(
      operationQueue.forall{
        case (null,_) => true
        case (op,_) => require(op.parents.forall(_.forall{case (n,e) => n.elements.contains(e)}),{
          // print info to see what went wrong
          "changed nodes: "+res.changedNodes+"\n"+
            "new nodes: "+res.newNodes+"\n\n"+
            op.parents.flatMap(_.filterNot{case (n,e)=>n.elements.contains(e)}).map{case (n,e)=>"bad parent: "+(n.index,e.prime.index, n.elements.map(_.prime.index))}.mkString("\n")
      })
          true
    })
  }

  private def updateOperationsWith(nodesToUpdate: Set[Int]): Unit = {
    removeOperationsWith(nodesToUpdate)
    addOperations(nodesToUpdate)
  }

  private def addOperations(nodes: Int => Boolean = _=>true): Unit = {
    // get all the nodes that need new operations
    val ops = PsddQueries.decisionNodesWithParents(root, node => nodeSelectionCriterion(node) && nodes(node.index))



    assert {
      val nonops = PsddQueries.decisionNodesWithParents(root, node => nodeSelectionCriterion(node) && !nodes(node.index))
      nonops.forall { case (node, parents) =>
        operationQueue.find(op => op._1 != null && op._1.node.index == node.index) match {
          case None =>
            val op = operationFinder.find((node, parents))
            if (op._2 > Double.NegativeInfinity) {
              false
            }
            else
              true
          case Some((null, _)) =>
            val op = operationFinder.find((node, parents))
            op._1 == null
          case Some(origOp) =>
            val op = operationFinder.find((node, parents))
            if ((origOp._2 > Double.NegativeInfinity || op._2 > Double.NegativeInfinity) && (
              !Util.isEqual(origOp._2, op._2) ||
                origOp._1.parents.toSet != op._1.parents.toSet ||
                !Util.isEqual(origOp._1.dll, op._1.dll) ||
                !Util.isEqual(origOp._1.dSize, op._1.dSize) ||
                origOp._1.changedNodes != op._1.changedNodes
              )) {
              println("FAIL")
              println(origOp._2 + " <=> " + op._2)
              println(origOp._1.parents.toSet + " <=> " + op._1.parents.toSet)
              println(origOp._1.dll + " <=> " + op._1.dll)
              println(origOp._1.dSize + " <=> " + op._1.dSize)
              println(origOp._1.changedNodes + " <=> " + op._1.changedNodes)
              false
            }
            else {
              true
            }
        }
      }
    }



    //find the best operation per node
    val newOps = ops.map{case (node, parents) =>
      val op = operationFinder.find((node, parents))
      if (op._1==null)
        Debug.writeln("ops",2,"no clone found for "+node.index)
      else
        Debug.writeln("ops",2,"add Clone("+node.index+"): "+op._1+"\t"+op._1.dll+"\t"+op._1.dSize+"\t"+op._2)
      op
    }.filterNot(_._1==null)

    //add the new operations
    operationQueue ++= newOps
  }

  override def removeOperationsWith(nodesToRemove: Set[Int]): Unit = {

    // find the nodes to remove
    val changedOps = operationQueue.filter(op => nodesToRemove.contains(op._1.node.index))
    changedOps.foreach(op =>Debug.writeln("ops",2,"remove "+op._1))

    // remove nodes
    changedOps.foreach(operationQueue -= _)
    assert(!operationQueue.exists(op => nodesToRemove.contains(op._1.node.index)), "some operations weren't deleted")
  }

  override def usesNonFlaggedNode(op: Clone): Boolean = !op.node.flag

}


/**
  * This class represents the queue of splits, with one clone per element
  * @param operationFinder class that finds the best operation for a certain element
  */
class SplitOperationQueue(operationFinder: SplitOperationFinder)
  extends  SpecificOperationQueue[Split, (PsddDecision, PsddElement)](operationFinder) {

  override def initialize(root: PsddDecision, psddManager: PsddManager, nodeSelectionCriterion: PsddDecision=>Boolean, addNewNodes: Boolean): Unit = {
    super.initialize(root, psddManager, nodeSelectionCriterion, addNewNodes)

    operationQueue = new mutable.TreeSet[(Split, Double)]()(Ordering.by(op => (-op._2, op._1.splitNode.index, op._1.splitElement.prime.index)))
    addOperations()
  }

  override def update(lastOp: PsddOperation, res: ExecutionResult): Unit = {
    super.update(lastOp, res)


    //////// Find all the nodes for which the operation should be (re)calculated ////////

    // nodes that were changed by last operation: update all their elements
    var nodesToUpdate = res.changedNodes

    // The nodes which operations affected at least one node that has changed: update all their elements
    nodesToUpdate ++= operationQueue.withFilter(op => res.changedNodes.exists(op._1.changedNodes.contains)).map(_._1.splitNode.index)

    // add the new nodes to the operations if this is desired
    if (addNewNodes) nodesToUpdate ++= res.newNodes


    ////////  update the operations of the nodes that should be updated ////////

    updateOperationsWith(nodesToUpdate)
  }

  private def updateOperationsWith(nodesToUpdate: Set[Int]): Unit = {
    removeOperationsWith(nodesToUpdate)
    addOperations(nodesToUpdate)
  }

  private def addOperations(nodes: Int => Boolean = _ => true): Unit = {
    // get all the nodes that need new operations
    val ops = PsddQueries.decisionNodesWithParents(root, node => nodeSelectionCriterion(node) && nodes(node.index))

    //find the best operation per node
    val newOps = ops.flatMap { case (node, _) => node.elements.map {element =>
      val op = operationFinder.find((node, element))
      if (op._1==null)
        Debug.writeln("ops",2,"no split found for "+(node.index, element.prime.index).toString())
      else
        Debug.writeln("ops", 2, "add Split(" + op._1.splitNode.index + ","+op._1.splitElement.prime.index+ "): " + op._1+"\t"+op._1.dll+"\t"+op._1.dSize+"\t"+op._2)
      op
    }}.filterNot(_._1 == null)


    assert {
      val nonops = PsddQueries.decisionNodesWithParents(root, node => nodeSelectionCriterion(node) && !nodes(node.index))
      nonops.forall { case (node, _) => node.elements.forall { element =>
        operationQueue.find(op => op._1 != null && op._1.splitNode.index == node.index && op._1.splitElement.prime.index == element.prime.index) match {
          case None =>
            val op = operationFinder.find((node, element))
            if (op._2 > Double.NegativeInfinity) {
              false
            }
            else
              true
          case Some((null, _)) =>
            val op = operationFinder.find((node, element))
            op._1 == null
          case Some(origOp) =>
            val op = operationFinder.find((node, element))
            if ((origOp._2 > Double.NegativeInfinity || op._2 > Double.NegativeInfinity) && (
              !Util.isEqual(origOp._2, op._2) ||
                origOp._1.splitElement != op._1.splitElement ||
                !Util.isEqual(origOp._1.dll, op._1.dll) ||
                !Util.isEqual(origOp._1.dSize, op._1.dSize) ||
                origOp._1.changedNodes != op._1.changedNodes
              )) {
              println("FAIL")
              println(origOp._2 + " <=> " + op._2)
              println(origOp._1.splitElement + " <=> " + op._1.splitElement)
              println(origOp._1.dll + " <=> " + op._1.dll)
              println(origOp._1.dSize + " <=> " + op._1.dSize)
              println(origOp._1.changedNodes + " <=> " + op._1.changedNodes)
              false
            }
            else {
              true
            }
          }
        }
      }
    }



    //add the new operations
    operationQueue ++= newOps
  }

  override def removeOperationsWith(nodesToRemove: Set[Int]): Unit = {

    // find the nodes to remove
    val changedOps = operationQueue.filter(op => nodesToRemove.contains(op._1.splitNode.index))
    changedOps.foreach(op => Debug.writeln("ops", 2, "remove " + op._1))

    // remove nodes
    changedOps.foreach(operationQueue -= _)
    assert(!operationQueue.exists(op => nodesToRemove.contains(op._1.splitNode.index)), "some operations weren't deleted")
  }

  override def usesNonFlaggedNode(op: Split): Boolean = !op.splitNode.flag
}

