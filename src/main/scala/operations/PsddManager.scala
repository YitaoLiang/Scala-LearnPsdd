package operations

import java.io.File

import algo.ParameterCalculator
import main.{Main, Output}
import sdd.{Sdd, SddManager}
import structure._
import util.{Util, Log, UniqueMap}

import scala.collection.mutable
import scala.io.Source

/**
 *
 * This class is responsible for low level transformations on PSDDs.
 * PSDDs can be read in from file (PSDD or SDD format) or a simple PSDD that represents independent variables can be generated for any vtree
 * 
 * If cache is turned on, it makes sure that we only keep unique psdds, considering two PSDDs equal that have the same structure and data.
 *
 */
class PsddManager(sddManager: SddManager, var cache: Boolean) {

  val trueSdd = new Sdd(true, sddManager)
  val trueNode = new PsddTrue(0, trueSdd)

  val roots = mutable.Set[PsddNode]()

  //////////////////////
  // MAKING NEW PSDDs //
  //////////////////////

  var count = 0 // 0 is true node

  private def nextIndex = {
    count += 1; count
  }

  // caches
  private val litCache = UniqueMap[Integer, PsddLiteral]()
  private val decisionCache = UniqueMap[Set[(Int,Int)], PsddDecision]()


  // make a literal psdd
  private def litPsdd: (VtreeNode, Boolean)=> PsddLiteral = if (cache) litPsddCache else litPsddNoCache

  private def litPsddCache(vtree: VtreeNode, pos:Boolean): PsddLiteral = vtree match {
    case vtree: VtreeVar =>
      val v = if (pos) vtree.v else -vtree.v
      litCache(v, PsddLiteral(nextIndex, vtree, v , new Sdd(v, sddManager)))

    case _ => throw new IllegalArgumentException("vtree must be a leaf node")
  }

  private def litPsddNoCache(vtree: VtreeNode, pos:Boolean): PsddLiteral = vtree match {
    case vtree: VtreeVar =>
      val v = if (pos) vtree.v else -vtree.v
      PsddLiteral(nextIndex, vtree, v , new Sdd(v, sddManager))

    case _ => throw new IllegalArgumentException("vtree must be a leaf node")
  }


  // make a decision node
  private def decisionPsdd: (VtreeInternal, Set[PsddElement], Sdd)=> PsddDecision = if (cache) decisionPsddCache else decisionPsddNoCache


  private def decisionPsddCache(vtree: VtreeInternal, elements: Set[PsddElement], formula: Sdd): PsddDecision = {
    val res = if (elements.forall(_.data.train.isEmpty) || (elements.size==1 && elements.head.prime.deterministic && elements.head.sub.deterministic)) {
      val res = decisionCache(elements.map(e => (e.prime.index, e.sub.index)), {
        PsddDecision(nextIndex, vtree, elements, formula)
      })
      elements.foreach{el =>val resEl = res.getElement(el.prime, el.sub).get; resEl.data = resEl.data.union(el.data)}
      assert(formula.equals(res.formula))
      res
    }
    else {
      PsddDecision(nextIndex, vtree, elements, formula)
    }
    res
  }

  private def decisionPsddNoCache(vtree: VtreeInternal, elements: Set[PsddElement], formula: Sdd): PsddDecision = {
    val res = PsddDecision(nextIndex, vtree, elements, formula)
    res
  }


  // make a true node for a certain variable
  private def truePsdd(vtree: VtreeInternalVar, data:DataSets, theta: Double) : PsddDecision = {
    val posLit = litPsdd(vtree.left, true)
    val negLit = litPsdd(vtree.left, false)
    val posData = data.filter(_(vtree.v))
    val negData = data.diff(posData)
    decisionPsdd(vtree, Set(PsddElement(posLit, trueNode, posData,posLit.formula, theta), PsddElement(negLit, trueNode, negData,negLit.formula, Log.oneMinus(theta))), trueSdd)
  }

  // after a split, the elements of a psdd change. This method updates the cache.
  private def updateDecisionPsdd(node: PsddDecision, oldElements: Set[PsddElement]): Unit = {
    if (node.noTrainData){
      println("updateDecisionPsdd")

      // remove old entry
      val oldElementsIntSet = oldElements.map(e=>(e.prime.index,e.sub.index))
      decisionCache.get(oldElementsIntSet) match {
        case Some(cachedOldNode) => if (cachedOldNode==node) decisionCache.remove(oldElementsIntSet)
        case None =>
      }
      val cachedNewNode = decisionCache(node.elements.map(e=>(e.prime.index, e.sub.index)), node)
      if (cachedNewNode!=node){
        cachedNewNode.elements.foreach{el =>
          val nel = node.elements.find(nel => nel.prime.index==el.prime.index) match {
            case None => throw new IllegalArgumentException("new node elements and cached node elements are not the same")
            case Some(nel) =>
              require(nel.prime==el.prime)
              require(nel.sub==el.sub)
              require(nel.data.train.isEmpty)
              require(el.data.train.isEmpty)
              require(Util.isEqual(nel.theta,el.theta))
              require(nel.formula==el.formula)
              el.data = el.data.union(nel.data)
          }
        }
        val parents = PsddQueries.decisionNodes(roots.toArray).flatMap(n=> n.elements.filter(e=>e.prime==node || e.sub==node).map(el => (n,el))).toSet
        updateDecisionPsddAfterDataChange(node, parents)
      }
    }
    require(PsddQueries.isValidDecisionNode(node), "The updated decision node is not valid!")
  }

  /**
   * If the node has no training data, then it should be in the cache.
   * If an equivalent node was already in the cache, then its parents should be redirected to this other node!
   * @param node
   * @param parents all the parents of the node
   */
  private def updateDecisionPsddAfterDataChange(node: PsddDecision, parents: => Set[(PsddDecision, PsddElement)]): Unit = {
    if (node.noTrainData) {
      val cacheNode = decisionCache(node.elements.map(e => (e.prime.index, e.sub.index)), node)
      if (cacheNode!=node) {
//        println("updateDecisionPsddAfterDataChange")
        val oldElements = parents.map{case (p,_) => p -> p.elements}.toMap
//        println(parents)
        parents.foreach{case (parentNode, parentEl) =>
          require(parentEl.prime == node || parentEl.sub==node, "not a parent! "+(parentNode.index, parentEl.prime.index))
          require(PsddQueries.isValidDecisionNode(parentNode))

          val (prime,sub) = if (parentEl.prime==node) (cacheNode, parentEl.sub) else (parentEl.prime, cacheNode)
          val newElement = PsddElement(parentEl.prime,parentEl.sub, parentEl.data, parentEl.formula, parentEl.theta)
          require(parentEl.prime.mc==newElement.prime.mc)
          require(parentEl.sub.mc==newElement.sub.mc)
          parentNode.elementSet -= parentEl
          parentNode.elementSet += newElement
        }
        oldElements.foreach{case (n,e) => updateDecisionPsdd(n,e)}
      }
    }
  }


  ////////////////
  // OPERATIONS //
  ////////////////


  /**
    * This method sets the clone specifications for a (multi) clone with maximum depth.
    * A clone specification is an object on a node that specifies how it should be cloned. This specifies for which
    * clones this specific node will be cloned. Because of the clone formula, not all nodes may need to be cloned.
    *
    * @param roots
    * @param dataFilters
    * @param cloneFormulas
    * @param maxDepth
    * @param withParents if this variable is true, the clone specification also keeps the parents of this node for each clone
    */
  private def setMaxDepthCloneSpecifications(roots: Array[PsddDecision], dataFilters: Array[Data], cloneFormulas: Array[Constraint], maxDepth: Int, withParents: Boolean): Unit = {
    assert(roots.forall(root => PsddQueries.decisionNodes(root).forall(_.cloneSpec==null)))
    val nbClones = dataFilters.length

    assert(nbClones == cloneFormulas.length)
    assert(roots.forall(_.vtree.level==roots.head.vtree.level))

    val maxLevel = if (roots.head.vtree.level + maxDepth <0) Int.MaxValue else roots.head.vtree.level + maxDepth

    val queue = mutable.Queue[PsddDecision]()
    roots.foreach{
      root=>
        root.cloneSpec = CloneSpecification(cloneAll = root.vtree.level <= maxLevel, cloneFormulas.map(c => c.isSatisfiableIn(root.formula)), if (withParents) Array.empty else null)
        queue.enqueue(root)

    }

    while(queue.nonEmpty) {
      val node = queue.dequeue()
      val localCloneFormulas = cloneFormulas.map(c => c.project(node.vtree.vars))

      val clones = node.cloneSpec.clones.zipWithIndex.filter(_._1)
      node.elements.foreach{ element => element match {
        case PsddElement(prime,sub,_,formula,_) =>

          clones.foreach{case (_,index) =>
            val cloneFormula = localCloneFormulas(index)

            if (cloneFormula.isSatisfiableIn(formula) &&(
              node.vtree.level < maxLevel  // not at maximum depth yet, so formula needs to be possible
              || !cloneFormula.isImpliedBy(formula)))// passed max depth, so element should have changed
            {

              Array(prime,sub).foreach{
                case child: PsddDecision =>
                  if (node.vtree.level < maxLevel || ! cloneFormula.project(child.vtree.vars).isImpliedBy(child.formula)) {
                    if (child.cloneSpec == null) {
                      queue.enqueue(child)
                      child.cloneSpec = CloneSpecification(cloneAll = node.vtree.level < maxLevel, Array.fill(nbClones)(false), if (withParents) Array.fill(nbClones)(mutable.Set.empty[(PsddDecision, PsddElement)]) else null)
                    }
                    assert(cloneFormula.project(child.vtree.vars).isSatisfiableIn(child.formula), "child not satisfiable")
                    child.cloneSpec.clones(index) = true
                    if (withParents) child.cloneSpec.parents(index) += ((node, element))
                  }
                case _ =>
              }
            }
          }
      }
      }
    }
  }


  /**
    * This method sets the clone specifications for a (multi) clone with a maximum number of edges.
    * A clone specification is an object on a node that specifies how it should be cloned. This specifies for which
    * clones this specific node will be cloned. Because of the clone formula, not all nodes may need to be cloned.
    *
    * @param roots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param maxEdges
    * @param minDll
    * @return
    */
  private def setMaxEdgesCloneSpecifications(roots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[Data], cloneFormulas: Array[Constraint], maxEdges: Int, minDll: Double): (Double,Int) = {
    // find the best way to clone
    setMaxDepthCloneSpecifications(roots,dataFilters,cloneFormulas,maxEdges,withParents=true)

    val clonables = clonableParentsBeforeChildren(roots)
    assert(clonables.forall(_.cloneSpec!=null))

    val cloneSpecs = mutable.TreeSet.empty[(PsddDecision, CloneSpecification, Double,Int, Double)](Ordering.by(a=>(-a._5,a._1.index)))

    def updateNode(node: PsddDecision): Unit = {
      assert(cloneSpecs.forall(spec1 => cloneSpecs.forall(spec2 => spec1==spec2 || spec1._1!=spec2._1)))
      val old = cloneSpecs.find(_._1.index==node.index)
      if (old.isDefined) cloneSpecs-=old.get
      assert(!cloneSpecs.exists(_._1.index==node.index))


      if (node.cloneSpec!=null) {
        if (node.cloneSpec.cloneAll) {
          if (node.elements.forall(el => (el.prime.cloneSpec == null || !el.prime.cloneSpec.cloneAll) && (el.sub.cloneSpec == null || !el.sub.cloneSpec.cloneAll))) {
            // clone all => 1 clone per formula
            if (cloneFormulas.forall(_.project(node.vtree.vars).restrict(node.formula) == node.formula)) {
              val oldCloneSpec = node.cloneSpec
              val newCloneSpec = CloneSpecification(cloneAll = false, oldCloneSpec.clones, oldCloneSpec.parents)
              val (oldLl, oldSize) = simulateClone(node, parameterCalculator, cloneFormulas, dataFilters)
              node.cloneSpec = newCloneSpec
              val (newLl, newSize) = simulateClone(node, parameterCalculator, cloneFormulas, dataFilters)
              node.cloneSpec = oldCloneSpec
              assert(node.cloneSpec != null)
              val dll = newLl - oldLl
              val dSize = newSize - oldSize
              cloneSpecs += ((node, newCloneSpec, dll, dSize,dll/dSize))
            }
          }
        }
        else {
          if (node.elements.forall(el => el.prime.cloneSpec == null && el.sub.cloneSpec == null)) {
            // 1 clone per formula => no clone
            if (cloneFormulas.forall(_.isImpliedBy(node.formula))) {
              val (oldLl, oldSize) = simulateClone(node, parameterCalculator, cloneFormulas, dataFilters)
              assert(node.cloneSpec != null)
              cloneSpecs += ((node, null, -oldLl, -oldSize, oldLl / oldSize))
            }
          }
        }
      }
    }

    var dll = 0.0
    var dSize = 0
    clonables.map( (node: PsddDecision) => simulateClone(node, parameterCalculator, cloneFormulas, dataFilters)).reduce( (a,b) => (a._1+b._1,a._2+b._2)) match {case (a,b) => dll =a; dSize = b}

    clonables.foreach(updateNode)
    while(dSize > maxEdges && cloneSpecs.nonEmpty){
      val head = cloneSpecs.head
      val (node, newSpecs, deltaLL, deltaSize, _) = head
      if (dll + deltaLL < minDll) {
        cloneSpecs.remove(head)
      } else {
        assert(deltaSize<=0, "got bigger somehow")
        dSize += deltaSize
        dll += deltaLL
        val oldSpecs = node.cloneSpec
        node.cloneSpec = newSpecs
        updateNode(node)
        oldSpecs.parents.foreach(_.foreach { case (parent, el) => updateNode(parent) })
      }
    }
    (dll, dSize)

  }

  /**
    * This method returns all the nodes that have a clone specification that are decendents of one of the given roots.
    * A node appears in the list before before its children
    * @param roots
    * @return
    */
  private def clonableParentsBeforeChildren(roots: Array[_ <: PsddNode]): mutable.ArrayBuffer[PsddDecision]= {
    val queue = mutable.ArrayBuffer[PsddDecision]()
    roots.foreach{
      case root: PsddDecision =>
        if (root.cloneSpec!=null) {
          queue += root
          root.flag = true
        }
      case _ =>
    }
    if (queue.nonEmpty) {
      var i = 0
      while (i < queue.size) {
        val node = queue(i)
        node.elements.foreach { el =>
          el.prime match {
            case child: PsddDecision =>
              if (!child.flag && child.cloneSpec != null) {
                queue += child
                child.flag = true
              }
            case _ =>
          }
          el.sub match {
            case child: PsddDecision =>
              if (!child.flag && child.cloneSpec != null) {
                queue += child
                child.flag = true
              }
            case _ =>
          }
        }
        i += 1
      }
    }
    queue.foreach(_.flag=false)
    queue
  }

  private def clonableChildrenBeforeParents(roots: Array[_ <: PsddNode]) = clonableParentsBeforeChildren(roots).reverse


  /**
    * Simulate a clone of one node according to the clone specifications set.
    * @param node
    * @param parameterCalculator
    * @param cloneFormulas
    * @param dataFilters
    * @return
    */
  private def simulateClone(node: PsddDecision, parameterCalculator: ParameterCalculator, cloneFormulas: Array[Constraint], dataFilters: Array[Data]): (Double,Int) = {

    // new elements

    val (dllNewElements,dSize, trainStayData) = if (node.cloneSpec.cloneAll || cloneFormulas.length <2) {
      var trainStayData = node.trainData
      var dll = 0.0
      var dSize = 0

      node.cloneSpec.clones.zipWithIndex.foreach {
        case (false, _) => Array.empty[Double]
        case (true, index) =>
          val newNodeTrainData = node.trainData.intersect(dataFilters(index))
          trainStayData = trainStayData.diff(newNodeTrainData)
          val cloneFormula = cloneFormulas(index).project(node.vtree.vars)
          val clonedElements =node.elements.filter{ el => cloneFormula.isSatisfiableIn(el.formula) }

          val deterministic = node.deterministic || cloneFormula.isDeterministic(node.formula,node.vtree.vars)
          val freebee = deterministic || newNodeTrainData.isEmpty || trainStayData.isEmpty

          clonedElements.foreach { el =>
            val newElTrainData = el.data.train.intersect(newNodeTrainData)
            val locDll = newElTrainData.total * parameterCalculator.calculate(newElTrainData, newNodeTrainData, cloneFormula.MC(el.formula), cloneFormula.MC(node.formula), clonedElements.size)
            dll +=locDll

            if (!freebee) dSize += 1
          }
      }
      (dll, dSize, trainStayData)

    } else {
      val cloneMap = mutable.Map.empty[Sdd,(Data, Constraint)]
      node.cloneSpec.clones.zipWithIndex.foreach{
        case (false,_) =>
        case (true, index) =>
          val c = cloneFormulas(index).project(node.vtree.vars)
          val f = c.restrict(node.formula)
          val v = cloneMap.get(f)
          val data = if (v.isDefined) dataFilters(index).union(v.get._1) else dataFilters(index)
          cloneMap(f) = (data, c)
      }
      var trainStayData = node.trainData
      var dll = 0.0
      var dSize = 0
      cloneMap.toArray.foreach{ case(f,(data, cloneFormula)) =>
        val newNodeTrainData = node.trainData.intersect(data)
        trainStayData = trainStayData.diff(newNodeTrainData)

        val deterministic = node.deterministic || cloneFormula.isDeterministic(node.formula,node.vtree.vars)
        val freebee = deterministic || newNodeTrainData.isEmpty || trainStayData.isEmpty

        val clonedElements = node.elements.filter(el => cloneFormula.isSatisfiableIn(el.formula))
        clonedElements.foreach{ el =>
          val newElTrainData = el.data.train.intersect(newNodeTrainData)
          dll += newElTrainData.total*parameterCalculator.calculate(newElTrainData, newNodeTrainData, cloneFormula.MC(el.formula), cloneFormula.MC(node.formula), clonedElements.size)

          if (!freebee) dSize += 1
        }
      }
      (dll, dSize, trainStayData)
    }

    // Staying element

    val oldLl = node.elements.toArray.map(el=> el.data.train.total*el.theta).sum

    val newNodeTrainData = node.trainData.intersect(trainStayData)
    val newLlOldNode = node.elements.toArray.map{el =>
      val newElTrainData = el.data.train.intersect(newNodeTrainData)
      newElTrainData.total*parameterCalculator.calculate(newElTrainData, newNodeTrainData, NoConstraint.MC(el.formula), NoConstraint.MC(node.formula), node.elements.size)
    }.sum


    (dllNewElements + newLlOldNode - oldLl, dSize)
  }


  /**
    * Simulate a max depth clone according to the clone specifications set.
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param maxDepth
    * @return
    */
  private def simulateCloneMaxDepth(cloneRoots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[Data], cloneFormulas: Array[Constraint], maxDepth: Int): SimulationResult = {
    setMaxDepthCloneSpecifications(cloneRoots, dataFilters, cloneFormulas, maxDepth, withParents = false)

    val clonables = clonableParentsBeforeChildren(cloneRoots)
    val (dll: Double, dSize: Int) = {
      if (clonables.isEmpty) (0.0,0)
      else clonables.map(node => simulateClone(node, parameterCalculator, cloneFormulas, dataFilters)).reduce( (a,b) => (a._1+b._1,a._2+b._2))
    }

    clonables.foreach(_.cloneSpec = null)

        val changedNodes = clonables.map(_.index).toSet
//    val changedNodes = clonables.map(_.index).toSet++clonables.flatMap(_.elements.flatMap(el=>Set(el.prime.index, el.sub.index)))

    SimulationResult(dll,dSize,changedNodes)
  }


  /**
    * * Simulate a max depth clone according to the clone specifications set.
    *
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param maxEdges
    * @param minDll
    * @return
    */
  private def simulateCloneMaxEdges(cloneRoots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[Data], cloneFormulas: Array[Constraint], maxEdges: Int, minDll: Double): SimulationResult = {
    val (dll,dSize) = setMaxEdgesCloneSpecifications(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, maxEdges, minDll)

    val clonables = clonableParentsBeforeChildren(cloneRoots)
    clonables.foreach(_.cloneSpec = null)


    val changedNodes = clonables.map(_.index).toSet
//    val changedNodes = clonables.map(_.index).toSet++clonables.flatMap(_.elements.flatMap(el=>Set(el.prime.index, el.sub.index)))

    SimulationResult(dll,dSize,changedNodes)
  }

  /**
    * Execute a clone of one node according to the clone specifications set.
    *
    * @param node
    * @param parameterCalculator
    * @param cloneFormulas
    * @param dataFilters
    * @param cloneMap
    * @return
    */
  private def executeClone(node: PsddDecision, parameterCalculator: ParameterCalculator, cloneFormulas: Array[Constraint], dataFilters: Array[DataSets], cloneMap: mutable.Map[PsddDecision, Array[PsddDecision]]): (Double, Double, Double,Int,Array[PsddDecision]) = {
    assert(node.cloneSpec!=null)


    def getClone(node: PsddNode, index: Int) = node match {
      case node: PsddDecision =>
        val opt = cloneMap.get(node)
        if (opt.isDefined) {
          opt.get(index)
        } else node
      case _=>node
    }

    val oldLl = node.elements.toArray.map{el => el.data.train.total*el.theta}.sum
    val oldValidLl = node.elements.toArray.map{el => el.data.valid.total*el.theta}.sum
    val oldTestLl = node.elements.toArray.map{el => el.data.test.total*el.theta}.sum

    // new elements

    val (newNodes: Array[PsddDecision], dllNewNodes, validDllNewNodes, testDllNewNodes, dSize) = if (node.cloneSpec.cloneAll || node.cloneSpec.clones.length<2) {
      var dll = 0.0
      var validDll = 0.0
      var testDll = 0.0
      var dSize = 0

      assert(node.cloneSpec.clones.zip(cloneFormulas).forall{case (clone, cloneFormula) => !clone || cloneFormula.project(node.vtree.vars).isSatisfiableIn(node.formula)},
        "We have a problem on node "+node.index+" with sdd "+node.formula.getVisualization+":\t"+node.cloneSpec.clones.zip(cloneFormulas).withFilter{case (clone, cloneFormula) =>
          clone && !cloneFormula.project(node.vtree.vars).isSatisfiableIn(node.formula)}.map{case (clone, cloneFormula) =>
          cloneFormula.project(node.vtree.vars)}.mkString("\t"))
      val nodes = node.cloneSpec.clones.zipWithIndex.map {
        case (false,_) => node
        case (true, index) =>
          val cloneFormula = cloneFormulas(index).project(node.vtree.vars)
          val newNodeData = node.data.intersect(dataFilters(index))
          val newNodeFormula = cloneFormula.restrict(node.formula)
          assert (cloneFormula.isSatisfiableIn(node.formula), "clone formula not satisfiable!")
          assert (!newNodeFormula.isFalse, "impossible node!")

          val deterministic = node.deterministic || cloneFormula.isDeterministic(node.formula,node.vtree.vars)
          val freebie = deterministic || newNodeData.train.isEmpty || node.trainData.size==newNodeData.train.size

          val cloneElements = node.elements.filter{el => cloneFormula.isSatisfiableIn(el.formula)}
          val newNodeElements = cloneElements.map { el =>
            val newElData = el.data.intersect(newNodeData)
            el.data = el.data.diff(newElData)
            val newElFormula = cloneFormula.restrict(el.formula)
            val newElTheta = parameterCalculator.calculate(newElData.train, newNodeData.train, NoConstraint.MC(newElFormula), NoConstraint.MC(newNodeFormula), cloneElements.size)

            val newPrime = el.prime match {case child: PsddDecision => getClone(child,index) case child => child}
            val newSub = el.sub match {case child: PsddDecision => getClone(child,index) case child => child}

            dll += newElData.train.total*newElTheta
            validDll += newElData.valid.total*newElTheta
            testDll += newElData.test.total*newElTheta

            if (!freebie) dSize += 1

            PsddElement(newPrime, newSub, newElData, newElFormula, newElTheta)
          }


          val clone =decisionPsdd(node.vtree, newNodeElements, newNodeFormula)
          clone
      }

      (nodes,dll, validDll, testDll,dSize)

    } else {

      val cloneMap = mutable.Map.empty[Sdd,(DataSets, Constraint, Int)]
      node.cloneSpec.clones.zipWithIndex.foreach{
        case (false,_) =>
        case (true, index) =>
          val c = cloneFormulas(index).project(node.vtree.vars)
          val f = c.restrict(node.formula)
          val v = cloneMap.get(f)
          val data = if (v.isDefined) dataFilters(index).union(v.get._1) else dataFilters(index)
          cloneMap(f) = (data, c, index)
      }

      var dll = 0.0
      var validDll = 0.0
      var testDll = 0.0
      var dSize = 0
      val cloneResults = cloneMap.map{ case(newNodeFormula,(data, cloneFormula,i)) =>

        val newNodeData = node.data.intersect(data)

        val deterministic = node.deterministic || cloneFormula.isDeterministic(node.formula,node.vtree.vars)
        val freebie = deterministic || newNodeData.train.isEmpty || node.trainData.size==newNodeData.train.size

        val cloneElements = node.elements.filter{el => cloneFormula.isSatisfiableIn(el.formula)}
        val newNodeElements = cloneElements.map { el =>
          val newElData = el.data.intersect(newNodeData)
          el.data = el.data.diff(newElData)
          val newElFormula = cloneFormula.restrict(el.formula)
          val newElTheta = parameterCalculator.calculate(newElData.train, newNodeData.train, NoConstraint.MC(newElFormula), NoConstraint.MC(newNodeFormula), cloneElements.size)
          val newPrime = el.prime match {
            case child: PsddDecision => getClone(child,i)
            case child => child
          }
          val newSub = el.sub match {
            case child: PsddDecision => getClone(child,i)
            case child => child
          }
          dll += newElData.train.total * newElTheta
          validDll +=newElData.valid.total * newElTheta
          testDll +=newElData.test.total * newElTheta

          if (!freebie) dSize += 1
          PsddElement(newPrime, newSub, newElData, newElFormula, newElTheta)
        }

        newNodeFormula ->decisionPsdd(node.vtree, newNodeElements, newNodeFormula)
      }

      val nodes = node.cloneSpec.clones.zip(cloneFormulas).map{
        case (false,_) => node
        case (true,c) => cloneResults(c.project(node.vtree.vars).restrict(node.formula))
      }
      (nodes,dll,validDll, testDll, dSize)
    }


    // recalculate parameters of old node
      node.elements.foreach{el => el.theta = parameterCalculator.calculate(el.data.train, node.trainData, Log.multiply(el.prime.mc,el.sub.mc), node.mc, node.elements.size)}

    val newLlOldNode = node.elements.toArray.map{el => el.data.train.total*el.theta}.sum
    val newValidLlOldNode = node.elements.toArray.map{el => el.data.valid.total*el.theta}.sum
    val newTestLlOldNode = node.elements.toArray.map{el => el.data.test.total*el.theta}.sum


    val dll = dllNewNodes + newLlOldNode - oldLl
    val validDll = validDllNewNodes + newValidLlOldNode - oldValidLl
    val testDll = testDllNewNodes + newTestLlOldNode - oldTestLl

    (dll,validDll, testDll, dSize,newNodes)
  }


  /**
    * Execute a multi clone according to the clone specifications set.
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param edgesToBeRemoved
    * @param root
    * @return
    */
  private def executeClones(cloneRoots: Array[PsddDecision], parameterCalculator:ParameterCalculator, dataFilters: Array[DataSets], cloneFormulas: Array[Constraint], edgesToBeRemoved: Set[(PsddDecision,PsddElement)], root: PsddDecision): (ExecutionResult, Array[Array[PsddDecision]]) = {

    val newNodes = mutable.Set[Int]()

    val cloneMap = mutable.Map[PsddDecision,Array[PsddDecision]]()
    val clonables = clonableChildrenBeforeParents(cloneRoots).toArray

    val (dll, validDll, testDll, dSize) = clonables.map { node =>
      val (dll, validDll, testDll, dSize, clones) = executeClone(node, parameterCalculator, cloneFormulas, dataFilters, cloneMap)
      cloneMap(node) = clones
      newNodes ++= clones.withFilter(_!=null).map(_.index)
      (dll, validDll, testDll,dSize)
    }.reduce( (a,b) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4))


    clonables.foreach(_.cloneSpec = null)



    def getParents(node:PsddDecision): Set[(PsddDecision, PsddElement)] = {
      if (node.baggage==null) {
        clonables.foreach(_.baggage = Set.empty[(PsddDecision, PsddElement)])
        val clonableIds = clonables.map(_.index).toSet
        PsddQueries.decisionNodes(root).foreach{node=>
          node.elements.foreach{el =>
            if (!edgesToBeRemoved.contains((node,el))) // skip parents that will be removed in the future
              Array(el.prime,el.sub).foreach(child => if (clonableIds.contains(child.index)) child.baggage = child.baggage.asInstanceOf[Set[(PsddDecision,PsddElement)]] + ((node,el)))
          }
        }
      }
      node.baggage.asInstanceOf[Set[(PsddDecision, PsddElement)]]
    }

    clonables.foreach(node => updateDecisionPsddAfterDataChange(node, getParents(node)))
    clonables.foreach(_.baggage=null)

    val rootClones = cloneRoots.map {root => cloneMap.get(root) match {
      case None => Array.fill(cloneFormulas.length)(root)
      case Some(clone) =>clone
    }}


      val changedNodes = clonables.map(_.index).toSet
//    val changedNodes = clonables.map(_.index).toSet++clonables.flatMap(_.elements.flatMap(el=>Set(el.prime.index, el.sub.index)))

    (ExecutionResult(dll,validDll, testDll, dSize, changedNodes, newNodes.toSet), rootClones)
  }

  /**
    * Execute a max depth multi clone according to the clone specifications set.
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param maxDepth
    * @param edgesToBeRemoved
    * @param root
    * @return
    */
  private def executeCloneMaxDepth(cloneRoots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[DataSets], cloneFormulas: Array[Constraint], maxDepth: Int, edgesToBeRemoved: Set[(PsddDecision,PsddElement)], root: PsddDecision): (ExecutionResult, Array[Array[PsddDecision]]) = {
    setMaxDepthCloneSpecifications(cloneRoots, dataFilters.map(_.train), cloneFormulas, maxDepth, withParents = false)
    executeClones(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, edgesToBeRemoved, root)
  }

  val minDll = 0.0000001

  /**
    * Execute a max edgesmulti clone according to the clone specifications set.
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param maxEdges
    * @param minDll
    * @param edgesToBeRemoved
    * @param root
    * @return
    */
  private def executeCloneMaxEdges(cloneRoots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[DataSets], cloneFormulas: Array[Constraint], maxEdges: Int, minDll: Double, edgesToBeRemoved: Set[(PsddDecision,PsddElement)], root: PsddDecision): (ExecutionResult, Array[Array[PsddDecision]]) = {
    setMaxEdgesCloneSpecifications(cloneRoots, parameterCalculator, dataFilters.map(_.train), cloneFormulas, maxEdges, minDll)
    executeClones(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, edgesToBeRemoved, root)
  }


  /**
    * Execute a constrained multi clone (used by split)
    * @param cloneRoots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param operationCompletionType
    * @param minDll
    * @param edgesToBeRemoved
    * @param root
    * @return
    */
  private def executeConstrainedClone(cloneRoots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[DataSets], cloneFormulas: Array[Constraint], operationCompletionType: OperationCompletionType, minDll: Double, edgesToBeRemoved: Set[(PsddDecision,PsddElement)], root: PsddDecision): (ExecutionResult, Array[Array[PsddDecision]]) = {
    operationCompletionType match {
      case Complete => executeCloneMaxDepth(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, Int.MaxValue, edgesToBeRemoved, root)
      case Minimal=> executeCloneMaxDepth(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, 0, edgesToBeRemoved, root)
      case MaxDepth(depth) => executeCloneMaxDepth(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, depth, edgesToBeRemoved, root)
      case MaxEdges(edges) => executeCloneMaxEdges(cloneRoots, parameterCalculator, dataFilters, cloneFormulas, edges, minDll, edgesToBeRemoved, root)
    }
  }

  /**
    * Simulate a constrained multi clone (used by split)
    * @param roots
    * @param parameterCalculator
    * @param dataFilters
    * @param cloneFormulas
    * @param operationCompletionType
    * @param minDll
    * @return
    */
  private def simulateConstrainedClone(roots: Array[PsddDecision], parameterCalculator: ParameterCalculator, dataFilters: Array[Data], cloneFormulas: Array[Constraint], operationCompletionType: OperationCompletionType, minDll: Double): SimulationResult = {
    operationCompletionType match {
      case Complete => simulateCloneMaxDepth(roots, parameterCalculator, dataFilters, cloneFormulas, Int.MaxValue)
      case Minimal=> simulateCloneMaxDepth(roots, parameterCalculator, dataFilters, cloneFormulas, 0)
      case MaxDepth(depth) => simulateCloneMaxDepth(roots, parameterCalculator, dataFilters, cloneFormulas, depth)
      case MaxEdges(edges) => simulateCloneMaxEdges(roots, parameterCalculator, dataFilters, cloneFormulas, edges, minDll)
    }
  }




  private def updateCompletionTypeAfterSplit(splitFormulas: Array[Constraint], operationCompletionType: OperationCompletionType): OperationCompletionType = {
    val updatedCompletionType = operationCompletionType match {
      case Complete => Complete
      case Minimal => Minimal
      case MaxDepth(k) => MaxDepth(k - 1)
      case MaxEdges(k) => MaxEdges(k - splitFormulas.length - 1)
    }
    updatedCompletionType
  }

  /**
    * Execute a split
    * @param splitNode
    * @param splitElement
    * @param parameterCalculator
    * @param splitFormulas
    * @param operationCompletionType
    * @param root
    * @return
    */
  def executeSplit(splitNode: PsddDecision, splitElement: PsddElement, parameterCalculator: ParameterCalculator, splitFormulas: Array[Constraint], operationCompletionType: OperationCompletionType, root:PsddDecision): ExecutionResult = {
    if (!splitElement.prime.isInstanceOf[PsddDecision]) return ExecutionResult(0.0,0.0,0.0,0,Set.empty[Int],Set.empty[Int])
    assert (splitNode.elements.contains(splitElement),"execution: element not in node\n"+splitElement+"\n"+splitNode.elements.mkString("\n"))
    val dataFilters = splitFormulas.map(f => splitElement.data.filter(f.isSatisfiedBy))

    ///////////////////
    // execute split //
    ///////////////////

    // get info about old elements and dll of this node
    val oldElements = splitNode.elements
    val oldDllSplit = splitNode.elements.toArray.map(el => el.data.train.total*el.theta).sum
    val oldValidDllSplit = splitNode.elements.toArray.map(el => el.data.valid.total*el.theta).sum
    val oldTestDllSplit = splitNode.elements.toArray.map(el => el.data.test.total*el.theta).sum
    val sizeBefore = splitNode.elements.size
    val nodeData = splitNode.data

    // remove old element
    splitNode.elementSet -= splitElement
    assert(splitNode.elements.size==sizeBefore-1, "Somehow not deleted")
    val newNbElements = splitNode.elements.size + splitFormulas.length


    // get information for new elements
    val nodeMC = NoConstraint.MC(splitNode.formula)
    val newElementsInfo = splitFormulas.zip(dataFilters).map {
      case (c, data) =>
        (data, c.restrict(splitElement.formula), parameterCalculator.calculate(data.train, nodeData.train, c.MC(splitElement.formula), nodeMC, newNbElements))
    }
    val dllNewElements = newElementsInfo.map{case (data,_,theta) => data.train.total*theta}.sum
    val validDllNewElements = newElementsInfo.map{case (data,_,theta) => data.valid.total*theta}.sum
    val testDllNewElements = newElementsInfo.map{case (data,_,theta) => data.test.total*theta}.sum


    // recalculate parameters and dll of the other elements
    splitNode.elements.foreach(el => el.theta = parameterCalculator.calculate(el.data.train, nodeData.train, Log.multiply(el.prime.mc,el.sub.mc), splitNode.mc, newNbElements))
    val newDllOtherElements = splitNode.elements.toList.map(el =>el.theta*el.data.train.total).sum
    val newValidDllOtherElements = splitNode.elements.toList.map(el =>el.theta*el.data.valid.total).sum
    val newTestDllOtherElements = splitNode.elements.toList.map(el =>el.theta*el.data.test.total).sum

    // calculate dll of executing split (without clone propagation)
    val splitDll = dllNewElements + newDllOtherElements - oldDllSplit
    val splitValidDll = validDllNewElements + newValidDllOtherElements - oldValidDllSplit
    val splitTestDll = testDllNewElements + newTestDllOtherElements - oldTestDllSplit

    ///////////////////
    // execute clone //
    ///////////////////
    val (cloneRes, clones) = executeConstrainedClone(Array(splitElement.prime.asInstanceOf[PsddDecision], splitElement.sub.asInstanceOf[PsddDecision]), parameterCalculator, dataFilters, splitFormulas, updateCompletionTypeAfterSplit(splitFormulas, operationCompletionType), - splitDll + minDll, Set((splitNode, splitElement)), root)

    require (Util.isEqual(Log.multiply(splitElement.prime.mc,splitElement.sub.mc), clones.head.zip(clones(1)).map{case (prime,sub) => Log.multiply(prime.mc,sub.mc)}.reduce(Log.add)),
    "problem with model counts!")

      // add new elements to node
      newElementsInfo.zip(clones.head.zip(clones(1))).foreach {
      case ((data,formula,theta), (prime,sub)) =>
          splitNode.elementSet += PsddElement(prime, sub, data, formula, theta)
    }

    //update the node in the cache
    updateDecisionPsdd(splitNode,oldElements)

    ExecutionResult(cloneRes.dll+splitDll,cloneRes.validDll+splitValidDll,cloneRes.testDll+splitTestDll,cloneRes.dSize+splitFormulas.length-1, cloneRes.changedNodes + splitNode.index, cloneRes.newNodes)
  }

  /**
    * Simulate a split
    *
   * Precondition: valid split! Every splitFormula should be satisfiable and not implied
   * @param splitNode
   * @param splitElement
   * @param parameterCalculator
   * @param splitFormulas
   * @param operationCompletionType
   * @return
   */
  def simulateSplit(splitNode: PsddDecision, splitElement: PsddElement, parameterCalculator: ParameterCalculator, splitFormulas: Array[Constraint], operationCompletionType: OperationCompletionType): SimulationResult = {
    require (splitFormulas.forall(constraint => constraint.isSatisfiableIn(splitElement.formula)),{
      "All the split formulas need to be satisfiable.\n"+
      "node: "+splitNode.index+", element: "+(splitElement.prime.index, splitElement.sub.index)+"\n"+
      splitFormulas.filterNot(constraint=>constraint.isSatisfiableIn(splitElement.formula)).map("constraint: "+_.toString).mkString("\n")
    })

    if (!splitElement.prime.isInstanceOf[PsddDecision]) return SimulationResult(0.0,0,Set.empty[Int])

    assert (splitNode.elements.contains(splitElement),"simulation: element not in node")
    val dataFilters = splitFormulas.map(f => splitElement.data.train.filter(f.isSatisfiedBy))

    //simulate split
    val oldDllSplit = splitNode.elements.toArray.map(el => el.data.train.total*el.theta).sum
    val newNbElements = splitNode.elements.size + splitFormulas.length -1
    val nodeMC = NoConstraint.MC(splitNode.formula)
    val newDllSplitNewEls = splitFormulas.zip(dataFilters).map{case (c,data) =>
      data.total*parameterCalculator.calculate(data,splitNode.trainData, c.MC(splitElement.formula), nodeMC, newNbElements)
    }.sum
    val newDllSplitOtherEls = (splitNode.elements-splitElement).toArray.map(el => el.data.train.total* parameterCalculator.calculate(el.data.train, splitNode.data.train, Log.multiply(el.prime.mc,el.sub.mc), splitNode.mc, newNbElements)).sum

    val splitDll = newDllSplitNewEls-oldDllSplit+newDllSplitOtherEls

    val cloneResult = simulateConstrainedClone(Array(splitElement.prime.asInstanceOf[PsddDecision],splitElement.sub.asInstanceOf[PsddDecision]), parameterCalculator, dataFilters, splitFormulas,
      updateCompletionTypeAfterSplit(splitFormulas, operationCompletionType), -splitDll+minDll)

    SimulationResult(cloneResult.dll + splitDll, cloneResult.dSize + splitFormulas.length - 1, cloneResult.changedNodes + splitNode.index)
  }

  /**
    * execute a clone
    * @param cloneNode
    * @param parameterCalculator
    * @param parents
    * @param operationCompletionType
    * @param root
    * @return
    */
  def executeClone(cloneNode: PsddDecision, parameterCalculator: ParameterCalculator, parents: Array[Set[(PsddDecision, PsddElement)]], operationCompletionType: OperationCompletionType, root: PsddDecision): ExecutionResult = {
    require (parents.forall(_.forall{case (n,e) => n.elements.contains(e)}), {
      "A clone cannot be done for an element that is not in the psdd\n" +
      parents.flatMap{_.map { case (n, e) =>
          "extra check: "+n.elements.contains(e)+"\n"+
          e + "\n+++ not in +++\n" +
          n.elements.mkString("\n")
      }}.mkString("\n")
    })

    val (res, clones) = executeConstrainedClone(Array(cloneNode), parameterCalculator, parents.map(_.map(_._2.data).reduce(_.union(_))), parents.map(_ => NoConstraint), operationCompletionType, minDll, parents.flatten.toSet, root)

    if (clones.forall(_.forall(_==cloneNode))) println("useless clone! All the parents are redirected to the old node")


    // redirect parents

    val parentsWithOldElements = parents.flatMap(_.map{case (parent,_) => parent -> parent.elements}).toMap

    parents.zip(clones.head).foreach{ case (parentss, node) =>
      parentss.foreach { case (parentNode, parentEl) =>
        if (node==cloneNode) println("clone = node")
        val (prime, sub) = if (parentNode.vtree.left == node.vtree) (node, parentEl.sub) else (parentEl.prime, node)
        val sizeBefore = parentNode.elements.size

        Array(prime,sub).foreach{
          case child: PsddDecision =>
            if (parentEl.data.intersect(child.data)!=parentEl.data) println("intersect",parentEl.data, child.data)
            if (!parentEl.data.diff(child.data).isEmpty) println("diff",parentEl.data, child.data)
          case _ =>
        }

        parentNode.elementSet -= parentEl
        if (parentNode.elements.size!=sizeBefore-1)
          println ("element somehow not deleted\n"+
            (parentEl.prime.index, parentEl.sub.index, parentEl.data.train.size, parentEl.formula.getId, parentEl.theta) +"\n+++ not in +++\n"+
            parentNode.elements.map(el=>(el.prime.index, el.sub.index, el.data.train.size, el.formula.getId, el.theta)).mkString("\n"))
        require(Util.isEqual(parentEl.prime.mc,prime.mc), "problem  with mc in prime: "+parentEl.prime.mc+" <=> "+prime.mc)
        require(Util.isEqual(parentEl.sub.mc,sub.mc), "problem  with mc in sub: "+parentEl.sub.mc+" <=> "+sub.mc)
        parentNode.elementSet += PsddElement(prime, sub, parentEl.data, parentEl.formula, parentEl.theta)
      }
    }

    // update all parents
    parentsWithOldElements.foreach{case (parentNode, oldElements)=> updateDecisionPsdd(parentNode, oldElements)}


    ExecutionResult(res.dll, res.validDll, res.testDll, res.dSize, res.changedNodes ++ parents.flatMap(_.map(_._1.index)), res.newNodes)
  }

  /**
    * simulate a clone
    * @param root
    * @param parameterCalculator
    * @param parents
    * @param operationCompletionType
    * @return
    */
  def simulateClone(root: PsddDecision, parameterCalculator: ParameterCalculator, parents: Array[Set[(PsddDecision, PsddElement)]], operationCompletionType: OperationCompletionType): SimulationResult = {
    val res = simulateConstrainedClone(Array(root), parameterCalculator, parents.map(_.map(_._2.data.train).reduce(_.union(_))), parents.map(_ => NoConstraint), operationCompletionType, minDll)
    SimulationResult(res.dll, res.dSize, res.changedNodes ++ parents.flatMap(_.map(_._1.index)))
  }




  ////////////////
  // PARAMETERS //
  ////////////////

  def calculateParameters(psdd: PsddNode, parameterCalculator: ParameterCalculator, root:PsddNode): Unit = PsddQueries.decisionNodes(psdd).foreach{ node =>
    val nodeTrainData = node.trainData
    val nbElements = node.elements.size
    node.elements.foreach{el =>
      el.theta = parameterCalculator.calculate(el.data.train, nodeTrainData, Log.multiply(el.prime.mc,el.sub.mc), node.mc, nbElements)
    }
    if(!Util.isEqual(node.elements.toArray.map(_.theta).reduceLeft(Log.add), Log.one)){
      println(node.elements.toArray.map(el=>math.pow(math.E,el.theta)).mkString(";")+"Sum"+node.elements.map(_.theta).reduce(Log.add).toString+"log"+node.elements.map(_.theta).mkString(";"))
      println(node.index)

      findNode(root,node.index)

      require(true==false, "elements parameters do not sum up to one\n")
    }
  }

  //findNode is used to debug in parameter calculations
  def findNode(current:PsddNode,target:Int):Boolean = {
    if (current.index == target){
      println(current.elements.size)
      current.elements.foreach{el=>
        println(el.formula.toString)
      }
      true
    }else{
      current match {
        case n:PsddDecision =>
          var found = false
          n.elements.foreach{el=>
            if (!found) {
              found = findNode(el.prime,target)
            }
            if (!found) {
              found = findNode(el.sub,target)
            }
          }
          found
        case _ => false
      }
    }
  }



  ////////////////////////
  // INITIALIZING PSDDs //
  ////////////////////////

  /**
    * Distribute data over a PSDD
    * @param psdd
    * @param data
    */
  def distributeData(psdd: PsddNode, data: DataSets): Unit = {

    val parentsBeforeChildren = PsddQueries.parentsBeforeChildren(psdd)

    val nodeTempData = mutable.Map[Int,DataSets]()

    parentsBeforeChildren.reverseIterator.foreach {

      case node: PsddDecision =>
        node.elements.foreach { el =>
          el match {
            case PsddElement(prime: PsddLiteral, sub: PsddTrue, _, _, _) =>
              el.data = data.filter(_(prime.v) == prime.pos)

            case PsddElement(prime: PsddDecision, sub: PsddDecision, _, _, _) =>
              el.data = nodeTempData(prime.index).intersect(nodeTempData(sub.index))
          }
        }
        nodeTempData(node.index) = node.elements.map(_.data).reduce(_.union(_))
      case _ =>
    }

    val nodeData = mutable.Map[Int,DataSets]()
    nodeData(psdd.index) = nodeTempData(psdd.index)

    parentsBeforeChildren.foreach { node =>
      val nData = nodeData(node.index)
      node.elements.foreach{ el =>
        el.data = nData.intersect(el.data)
        nodeData(el.prime.index) = el.data.union(nodeData.getOrElse(el.prime.index, data.empty))
        nodeData(el.sub.index) = el.data.union(nodeData.getOrElse(el.sub.index, data.empty))
      }
    }
  }

  /**
   * This method rebuilds the psdd. If cache is on, the nodes will be put in cache and reused if necessary.
   * This is useful when the PSDD was build with cache turned off.
   * @param psdd the psdd to rebuild
   * @return that was rebuild psdd
   */
  private def rebuildPsdd(psdd: PsddNode): PsddNode = {
    val p2p = mutable.Map[PsddNode,PsddNode]()
    PsddQueries.childrenBeforeParents(psdd).foreach(node => p2p(node)=node match {
      case node: PsddDecision => decisionPsdd(node.vtree, node.elements.map{el => new PsddElement(p2p(el.prime),p2p(el.sub), el.data, el.formula, el.theta)}, node.formula)
      case node: PsddLiteral => litPsdd(node.vtree,node.pos)
      case node: PsddTrue => node
    })

//    Main.cloneNode = p2p(Main.cloneNode).asInstanceOf[PsddDecision]
//    Main.parent1 = p2p(Main.parent1).asInstanceOf[PsddDecision]
//    Main.prime1 = p2p(Main.prime1).asInstanceOf[PsddDecision]
//    Main.parent2 = p2p(Main.parent2).asInstanceOf[PsddDecision]
//    Main.prime2 = p2p(Main.prime2).asInstanceOf[PsddDecision]

    p2p(psdd)
  }

  /**
   * Read in a PSDD, optionally recalculating the parameters
    *
   * @param file the psdd file
   * @param vtree the vtree
   * @param data the data do distribute in the psdd
   * @param parameterCalculator if null, parameters are not recalculated
   * @return the psdd that was read from the file
   */
  def readPsdd(file: File, vtree: VtreeInternal, data: DataSets, parameterCalculator: ParameterCalculator = null): PsddNode = {
    val float = "[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?"
    val comment = "[cC].*".r
    val psdd = "[pP]sdd (\\d+)".r
    val posLiteral = "[lL] (\\d+) (\\d+) (\\d+)".r
    val negLiteral = "[lL] (\\d+) (\\d+) -(\\d+)".r
    val truePsddNode = ("[tT] (\\d+) (\\d+) (\\d+) (" + float + ")").r
    val decision = "[dD] (\\d+) (\\d+) (\\d+) (.*)".r

    val cacheBefore = cache
    cache=false

    val nodes = mutable.Map.empty[Int,PsddDecision]
    var rootId = ""

    for (line <- Source.fromFile(file).getLines()){
      line match {
        case comment() => //println("comment")
        case psdd(nbNodes) => //tln("psdd", nbNodes)

        case posLiteral(id, vtreeId, v) => //println("pos literal", id, vtreeId, v)
          val vtreeNode = vtree.get(vtreeId.toInt)
          require(vtreeNode.asInstanceOf[VtreeInternalVar].v == v.toInt)
          val pos = litPsdd(vtreeNode.left, true)
          nodes(id.toInt) = decisionPsdd(vtreeNode, Set( PsddElement(pos,trueNode, data,pos.formula, Log.one)),pos.formula)
          rootId = id
        case negLiteral(id, vtreeId, v) => //println("neg literal", id, vtreeId, v)
          val vtreeNode = vtree.get(vtreeId.toInt)
          require(vtreeNode.asInstanceOf[VtreeInternalVar].v == v.toInt)
          val neg = litPsdd(vtreeNode.left, false)
          nodes(id.toInt) = decisionPsdd(vtreeNode, Set( PsddElement(neg,trueNode, data,neg.formula, Log.one)),neg.formula)
          rootId = id
        case truePsddNode(id, vtreeId, v, litProb) => //println("trueNode", id, vtreeId, v, litProb)
          val vtreeNode = vtree.get(vtreeId.toInt).asInstanceOf[VtreeInternalVar]
          require(vtreeNode.v == v.toInt)
          nodes(id.toInt) = truePsdd(vtreeNode, data, litProb.toDouble)
          rootId = id
        case decision(id, vtreeId, nbElements, elements) => //println("decision", id, vtreeId, nbElements, elements)
          val vtreeNode = vtree.get(vtreeId.toInt)
          val nodeElements = elements.split(" ").grouped(3).map{ar =>
            val prime = nodes(ar(0).toInt)
            val sub = nodes(ar(1).toInt)
            new PsddElement(prime, sub,data,prime.formula.conjoin(sub.formula), ar(2).toDouble)}.toSet
          assert(nodeElements.size == nbElements.toInt)
          require(nodeElements.forall(el => el.prime.vtree == vtreeNode.left && el.sub.vtree == vtreeNode.right))
          val formula = nodeElements.map(_.formula).reduce(_.disjoin(_))
          nodes(id.toInt) = decisionPsdd(vtreeNode, nodeElements, formula)
          rootId = id
      }
    }

    val root = nodes(rootId.toInt)
    distributeData(root, data)

    cache = cacheBefore
    val res = if (cache) rebuildPsdd(root) else root
    if (parameterCalculator!= null) calculateParameters(res, parameterCalculator, res)
    roots += res
    res
  }

  var totalVtreeLevels = 0

  /**
    * Read an SDD to a PSDD and learn the parameters using the provided parameter calculator
    * @param file
    * @param vtree
    * @param data
    * @param parameterCalculator
    * @return
    */
  def readPsddFromSdd(file: File, vtree: VtreeInternal, data: DataSets, parameterCalculator: ParameterCalculator): PsddNode = {
    val comment = "[cC].*".r
    val sdd = "[sS]dd (\\d+)".r
    val posLiteral = "[lL] (\\d+) (\\d+) (\\d+)".r
    val negLiteral = "[lL] (\\d+) (\\d+) -(\\d+)".r
    val trueSddNode = "[tT] (\\d+)".r
    val falseSdd = "[fF] (\\d+)".r
    val decision = "[dD] (\\d+) (\\d+) (\\d+) (.*)".r

    val trueNodes = mutable.Map[VtreeNode,PsddNode]()
    def getTrueNode(vtree: VtreeNode): PsddNode = {
      trueNodes.getOrElseUpdate(vtree, vtree match {
        case vtree: VtreeInternalVar => truePsdd(vtree, data, math.log(0.5))
        case vtree: VtreeInternal => decisionPsdd(vtree,Set(PsddElement(getTrueNode(vtree.left),getTrueNode(vtree.right),data,trueSdd,Log.one)),trueSdd)
      })
    }

    def fillTillLevel(nodes: mutable.Map[Int, PsddDecision], level: Int): PsddNode = {

      val (minLevel, minLevelNode) = nodes.minBy(_._1)
      var curLevel = minLevel
      var curNode = minLevelNode
      while (curLevel>level){
        val parent = curNode.vtree.asInstanceOf[VtreeInternal].parent
        assert (parent.level == curLevel-1)
        val (prime,sub) = if (curNode.vtree.index<parent.index) (curNode, getTrueNode(parent.right)) else (getTrueNode(parent.left),curNode)
        curNode = decisionPsdd(parent,Set(PsddElement(prime,sub,data,curNode.formula,Log.one)),curNode.formula)
        curLevel-=1
        nodes(curLevel) = curNode
      }
      curNode
    }


    val cacheBefore = cache
    cache=false

    var nodes: Array[mutable.Map[Int,PsddDecision]] = null
    var rootId = ""
    var falseId = -1
    var trueId = -1

    for (line <- Source.fromFile(file).getLines()){
//      println(line)
      line match {
        case comment() => //println("comment")
        case sdd(nbNodes) => //println("psdd", nbNodes)
          nodes = new Array(nbNodes.toInt)
        case posLiteral(id, vtreeId, v) => //println("pos literal", id, vtreeId, v)
          val vtreeNode = vtree.get(vtreeId.toInt).asInstanceOf[VtreeInternalVar]
          require(vtreeNode.v == v.toInt)
          val pos = litPsdd(vtreeNode.left, true)
          nodes(id.toInt) = mutable.Map(vtreeNode.level ->decisionPsdd(vtreeNode, Set( PsddElement(pos,trueNode, data,pos.formula, Log.one)),pos.formula))
          rootId = id
        case negLiteral(id, vtreeId, v) => //println("neg literal", id, vtreeId, v)
          val vtreeNode = vtree.get(vtreeId.toInt).asInstanceOf[VtreeInternalVar]
          require(vtreeNode.v == v.toInt)
          val neg = litPsdd(vtreeNode.left, false)
          nodes(id.toInt) = mutable.Map(vtreeNode.level ->decisionPsdd(vtreeNode, Set( PsddElement(neg,trueNode, data,neg.formula, Log.one)),neg.formula))
          rootId = id
        case trueSddNode(id) => //println("trueNode", id, vtreeId, v, litProb)
          trueId = id.toInt
          rootId = id
        case falseSdd(id) => //println("trueNode", id, vtreeId, v, litProb)
          falseId = id.toInt
          rootId = id
        case decision(id, vtreeId, nbElements, elements) => //println("decision", id, vtreeId, nbElements, elements)
          val vtreeNode = vtree.get(vtreeId.toInt)
          val nodeElementsIds = elements.split(" ").grouped(2).map(ar=>(ar(0).toInt,ar(1).toInt)).filterNot(_._2==falseId).toArray
          assert(nodeElementsIds.nonEmpty)
          val realNbElements = nodeElementsIds.length
          val nodeElements = nodeElementsIds.map{case (primeId, subId) =>
            val prime = if (primeId==trueId) getTrueNode(vtreeNode.left) else nodes(primeId).getOrElse(vtreeNode.level+1,fillTillLevel(nodes(primeId),vtreeNode.level+1))
            val sub = if (subId==trueId) getTrueNode(vtreeNode.right) else nodes(subId).getOrElse(vtreeNode.level+1,fillTillLevel(nodes(subId),vtreeNode.level+1))
            assert(vtreeNode.left.asInstanceOf[VtreeInternal].level==vtreeNode.level+1)
            require(prime.vtree==vtreeNode.left)
            require(sub.vtree==vtreeNode.right)
            new PsddElement(prime, sub,data,prime.formula.conjoin(sub.formula), math.log(1.0/realNbElements.toDouble))}.toSet

          assert(nodeElements.nonEmpty)
          val formula = nodeElements.map(_.formula).reduce(_.disjoin(_))
          nodes(id.toInt) = mutable.Map(vtreeNode.level -> decisionPsdd(vtreeNode, nodeElements, formula))
          if (vtreeNode.level>totalVtreeLevels) totalVtreeLevels=vtree.level
          rootId = id
      }
    }

    val root = if (rootId.toInt==trueId) getTrueNode(vtree) else nodes(rootId.toInt).getOrElse(vtree.level, fillTillLevel(nodes.last, vtree.level))
    distributeData(root, data)

    cache = cacheBefore
    val res = if (cache) rebuildPsdd(root) else root
    calculateParameters(res,parameterCalculator, res)

    roots += res
    res
  }

  /**
    * Make a PSDD for the given vtree that represents independent variables (the product of marginals)
    * @param vtree
    * @param data
    * @param parameterCalculator
    * @return
    */
  def newPsdd(vtree: VtreeInternal, data: DataSets, parameterCalculator: ParameterCalculator): PsddDecision = {
    val vtree2psdd = mutable.Map[VtreeNode, PsddDecision]()

    for (vtree <- vtree.parentsBeforeChildren().reverse) vtree2psdd(vtree) = vtree match {
      case vtree: VtreeInternalVar =>
        if (vtree.level>totalVtreeLevels) totalVtreeLevels=vtree.level
        val pos = litPsdd(vtree.left, true)
        val neg = litPsdd(vtree.left, false)
        decisionPsdd(vtree, Set(
        PsddElement(pos,trueNode, data,pos.formula, math.log(0.5)),
        PsddElement(neg,trueNode, data,neg.formula, math.log(0.5))),trueSdd)

      case vtree: VtreeInternal =>
        val elements = Set(new PsddElement(vtree2psdd(vtree.left), vtree2psdd(vtree.right), data, trueSdd, Log.one))
        decisionPsdd(vtree, elements, trueSdd)
    }
    val root = vtree2psdd(vtree)
    distributeData(root, data)
    calculateParameters(root, parameterCalculator, root)
    roots += root
    root
  }

}
