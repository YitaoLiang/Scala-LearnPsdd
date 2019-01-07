package operations

import java.io.{File, PrintWriter}

import sdd.Sdd
import structure._
import util.{Util, Log}

import BigDecimal._
import spire.math._

import scala.collection.mutable

object PsddQueries {


  def parentsBeforeChildren(root: PsddNode): mutable.ArrayBuffer[PsddNode]= {
    val queue = new mutable.ArrayBuffer[PsddNode]
    queue += root
    root.flag=true
    var i = 0
    var truePsdd: PsddNode = null
    while (i < queue.size){
      val node = queue(i)
      node.elements.foreach { el =>
        if (!el.prime.flag) {
          queue += el.prime
          el.prime.flag = true
        }
        if (!el.sub.flag) {
          if (el.sub.isInstanceOf[PsddTrue]) truePsdd = el.sub
          else queue += el.sub
          el.sub.flag = true
        }
      }
      i+=1
    }
    queue += truePsdd
    queue.foreach(_.flag=false)
    queue
  }

  def decisionParentsBeforeChildren(root: PsddNode, maxLevel: Int = Int.MaxValue): mutable.ArrayBuffer[PsddDecision]= decisionParentsBeforeChildren(Array(root), maxLevel)

  def decisionParentsBeforeChildren(roots: Array[PsddNode], maxLevel: Int): mutable.ArrayBuffer[PsddDecision]= {
    val queue = mutable.ArrayBuffer[PsddDecision]()
    roots.foreach{_ match {
      case root: PsddDecision => queue+=root; root.flag=true
    }}
    var i = 0
    while (i < queue.size){
      val node = queue(i)
      if (node.vtree.level < maxLevel) {
        node.elements.foreach { el =>
          el.prime match {
            case child: PsddDecision =>
              if (!child.flag) {
                queue += child
                child.flag = true
              }
            case _ =>
          }
          el.sub match {
            case child: PsddDecision =>
              if (!child.flag) {
                queue += child
                child.flag = true
              }
            case _ =>
          }
        }
      }
      i+=1
    }

    queue.foreach(_.flag=false)
    queue
  }

  def decisionNodesWithParents(root: PsddNode, selectCrit: PsddDecision => Boolean = _=>true): mutable.Map[PsddDecision,Array[(PsddDecision,PsddElement)]] = {
    val nodes = mutable.Map[PsddDecision, Array[(PsddDecision, PsddElement)]]()
    val queue = mutable.ArrayBuffer[PsddDecision]()
    root match {
      case root:PsddDecision =>
        if (selectCrit(root)) {nodes += root->Array.empty}
        queue += root
        root.flag=true
        var i = 0
        while (i < queue.size){
          val node = queue(i)
          node.elements.foreach { el =>
            el.prime match {
              case child: PsddDecision =>
                if (!child.flag) {
                  queue += child
                  child.flag = true
                }
                if (selectCrit(child)) {nodes(child) = nodes.getOrElse(child, Array.empty) :+ (node,el)}
              case _ =>
            }
            el.sub match {
              case child: PsddDecision =>
                if (!child.flag) {
                  queue += child
                  child.flag = true
                }
                if (selectCrit(child)) {nodes(child) = nodes.getOrElse(child, Array.empty) :+ (node,el)}
              case _ =>
            }
          }
        i+=1
      }
    }
    queue.foreach(_.flag=false)
    nodes
  }

  def nodes(root: PsddNode) = parentsBeforeChildren(root)
  def decisionNodes(root: PsddNode) = decisionParentsBeforeChildren(root)
  def decisionNodes(roots: Array[PsddNode]) = decisionParentsBeforeChildren(roots, Int.MaxValue)

  def childrenBeforeParents(root: PsddNode) = parentsBeforeChildren(root).reverse
  def decisionChildrenBeforeParents(root: PsddNode) = decisionParentsBeforeChildren(root).reverse

  def elements(root: PsddNode) = parentsBeforeChildren(root).flatMap(_.elements)

  def size(psdd: PsddDecision) = elements(psdd).size

  def logLikelihood(psdd: PsddNode, dataSet: String): Double = dataSet match {
    case "train" => elements(psdd).map(el => el.theta*el.data.train.total).sum
    case "test" => elements(psdd).map(el => el.theta*el.data.test.total).sum
    case "valid" => elements(psdd).map(el => el.theta*el.data.valid.total).sum
  }

  def entropy(root: PsddNode): Double = {
    val childrenBeforeParentsNodes =childrenBeforeParents(root)
    childrenBeforeParentsNodes.foreach(node => node.baggage = node.elements.map{el=> el.theta - el.prime.baggage.asInstanceOf[Double] - el.sub.baggage.asInstanceOf[Double]})
    val ent = root.baggage.asInstanceOf[Double]
    childrenBeforeParentsNodes.foreach(_.baggage=null)
    ent
  }

  def logProb(root: PsddNode, assignment: Map[Int,Boolean]): Double = {
    val childrenBeforeParentsNodes = childrenBeforeParents(root)
    childrenBeforeParentsNodes.foreach{node =>
      node.baggage = node match {
        case node: PsddLiteral => if (assignment.getOrElse(node.v,node.pos)==node.pos) Log.one else Log.zero
        case node: PsddTrue => Log.one
        case node: PsddDecision => node.elements.map(el=>Log.multiply(el.prime.baggage.asInstanceOf[Double],el.sub.baggage.asInstanceOf[Double],el.theta)).reduce(Log.add)
      }
    }
    val prob = root.baggage.asInstanceOf[Double]
    childrenBeforeParentsNodes.foreach(_.baggage=null)
    prob
  }

  def logProb(root: PsddNode, formula: Sdd): Double = {
    0.0 //TODO
  }

  def bigDecimalProb(root: PsddNode, assignment: Map[Int,Boolean]): BigDecimal = {
    val logProbability = logProb(root,assignment)
    spire.math.pow(BigDecimal.decimal(math.E),BigDecimal.decimal(logProbability))
  }


  def setContextsAsBaggage(root: PsddNode): Unit = {
    val nodes = parentsBeforeChildren(root)
    nodes.foreach(_.baggage = new Sdd(false,root.formula.getManager))
    root.baggage = new Sdd(true,root.formula.getManager)
    nodes.foreach{node =>
      val nodeContext = node.baggage.asInstanceOf[Sdd]
      node.elements.foreach { case PsddElement(prime, sub, _, _, _) =>
        val elementContext = nodeContext.conjoin(prime.formula)
        prime.baggage = prime.baggage.asInstanceOf[Sdd].disjoin(elementContext)
        sub.baggage = sub.baggage.asInstanceOf[Sdd].disjoin(elementContext)
      }
    }
  }

  def isValidDecisionNode(node: PsddDecision): Boolean = {
    // needs elements
    if (node.elements.isEmpty){
      println("Decision node "+node.index+" has no elements!")
      false
    }

      //data
    else if (!node.elements.forall{el =>
      val primeOk = el.prime match {
        case child: PsddDecision =>
          val ok = el.data.diff(child.data).isEmpty
          if (!ok) {
            println("Child "+child.index+" of element with prime "+el.prime.index+" of node "+node.index+" does not contain all the data of the element")
          }
          ok
        case _ => true
      }
      val subOk = el.sub match {
        case child: PsddDecision =>
          val ok = el.data.diff(child.data).isEmpty
          if (!ok) {
            println(el.data.train.size, child.data.train.size)
            println(el.data.intersect(child.data), el.data)
            println("Child "+child.index+" of element with prime "+el.prime.index+" of node "+node.index+" does not contain all the data of the element")
          }
          ok
        case _ => true
      }
      primeOk&&subOk
    }){
      false
    }

    // model count
    else if (!Util.isEqual(node.mc,node.elements.toList.map{case PsddElement(prime,sub,_,_,_) => Log.multiply(prime.mc,sub.mc)}.reduce(Log.add))) {
      println("Model count of node "+node.index+" is incorrect")
      println("node model count: "+node.mc)
      println("Elements model counts: "+ node.elements.toList.map(el=>"["+math.exp(el.prime.mc)+","+math.exp(el.sub.mc)+"]").mkString("{",",","}"))
      false
    }
    // parameters
    else if(!Util.isEqual(node.elements.toList.map(_.theta).reduce(Log.add), Log.one)){
      println("Parameters of node "+node.index+" do not sum up to one")
      println(node.elements.toList.map(_.theta).mkString("{",",","}"))
      false
    }

    //formulas
    else if (node.elements.map(_.formula).reduce(_.disjoin(_))!=node.formula){
      println("The formula of node "+node.index+" is not equal to the disjunction of the formulas of its elements.")
      println("Node formula: "+node.formula.getVisualization)
      println("Element formulas:")
      node.elements.foreach(el => println("\t"+el.formula.getVisualization))
      false
    }
    else if (!node.elements.forall{el=>
      val ok = el.prime.formula.conjoin(el.sub.formula)==el.formula
      if (!ok) {


        println("The formula of element with prime "+el.prime.index+" of node "+node.index+" is not the conjunction of the formula of the prime and sub.")
        println("Prime formula: "+el.prime.formula.getVisualization)
        println("Sub formula: "+el.sub.formula.getVisualization)
        println("Element formula: "+el.formula.getVisualization)
        println("Element projected on prime: "+el.formula.project(el.prime.vtree.vars.map(_.toLong)).getVisualization)
        println("Element projected on sub: "+el.formula.project(el.sub.vtree.vars.map(_.toLong)).getVisualization)
      }
      ok
    }){
      false
    }
    else if(!node.elements.subsets(2).map(_.toArray).forall(els => els.head.formula.conjoin(els(1).formula).isFalse)){
      println("Elements in node "+node.index+" are not mutually exclusive.")
      node.elements.foreach(el =>println(el.formula.getVisualization))
      false
    }
    else true

  }

  def isValid(root: PsddNode):Boolean = {
    setContextsAsBaggage(root)
    val psddNodes = nodes(root)
    val valid = psddNodes.forall{
      case node: PsddDecision =>

        isValidDecisionNode(node) && {

        // context

          val nodeContext = node.baggage.asInstanceOf[Sdd]
          node.elements.forall{el =>
            val selectionFormula = el.formula.conjoin(nodeContext)
            val selectedData = root.asInstanceOf[PsddDecision].data.filter(assignment => ConjunctionConstraint(assignment).MC(selectionFormula)>Log.zero)
            val ok = el.data == selectedData
            if (!ok){
              println("Data in element with prime "+el.prime.index+" of node "+node.index+" is not correct. "+el.data+" <=> "+selectedData)
              println("Selection formula: "+selectionFormula.getVisualization)
              println("Root data "+root.asInstanceOf[PsddDecision].data)
            }
            ok
          }
        }


      case _ => true
    }

    psddNodes.foreach(_.baggage=null)
    valid
  }

  def save(root: PsddNode, file: File): Unit = {
    val childrenBeforeParentNodes = decisionChildrenBeforeParents(root)
    val pw = new PrintWriter(file)
    pw.write("c ids of psdd nodes start at 0\nc psdd nodes appear bottom-up, children before parents\nc\nc file syntax:\nc psdd count-of-sdd-nodes\nc L id-of-literal-sdd-node literal \nc T id-of-trueNode-sdd-node id-of-vtree trueNode variable log(litProb)\nc D id-of-decomposition-sdd-node id-of-vtree number-of-elements {id-of-prime id-of-sub log(elementProb)}*\nc")
    pw.write("\npsdd "+childrenBeforeParentNodes.size)


    for (node<-childrenBeforeParentNodes) {
      node.vtree match {
        case vtree: VtreeInternalVar =>
          if (node.elements.size==1) pw.write("\nL "+node.index+" "+vtree.index+" "+node.elements.head.prime.asInstanceOf[PsddLiteral].literal)
          else {
            val pr = node.elements.find(_.prime.asInstanceOf[PsddLiteral].pos).get.theta
            pw.write("\nT "+node.index+" "+vtree.index+" "+vtree.v+" "+pr)
          }
        case vtree: VtreeInternal => pw.write("\nD "+node.index+" "+node.vtree.index+" "+node.elements.size+" "+node.elements.map{case el => el.prime.asInstanceOf[PsddDecision].index+" "+el.sub.asInstanceOf[PsddDecision].index+" "+el.theta}.mkString(" "))
      }
    }

    pw.flush()
    pw.close()
  }

  def saveAsSdd(root: PsddNode, file: File): Unit = {
    val childrenBeforeParentNodes = decisionChildrenBeforeParents(root)
    val pw = new PrintWriter(file)
    pw.write("c ids of sdd nodes start at 0\nc sdd nodes appear bottom-up, children before parents\nc\nc file syntax:\nc sdd count-of-sdd-nodes\nc T id-of-true-sdd-node \nc L id-of-literal-sdd-node id-of-vtree literal\nc D id-of-decomposition-sdd-node id-of-vtree number-of-elements {id-of-prime id-of-sub}*\nc")
    pw.write("\nsdd "+childrenBeforeParentNodes.size)

    val indexMap = childrenBeforeParentNodes.zipWithIndex.map{case(node, i) => node.index -> i}.toMap

    for ((node,i)<-childrenBeforeParentNodes.zipWithIndex) {
      node.vtree match {
        case vtree: VtreeInternalVar =>
          if (node.elements.size==1) pw.write("\nL "+i+" "+vtree.index+" "+node.elements.head.prime.asInstanceOf[PsddLiteral].literal)
          else pw.write("\nT " + i)

        case vtree: VtreeInternal => pw.write("\nD "+i+" "+node.vtree.index+" "+node.elements.size+" "+node.elements.map{case el => indexMap(el.prime.asInstanceOf[PsddDecision].index)+" "+indexMap(el.sub.asInstanceOf[PsddDecision].index)}.mkString(" "))
      }
    }

    pw.flush()
    pw.close()
  }



  val alphabet = {val letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; (1 to 26).map(i=> (i, letters(i-1).toString)).toMap}
  def saveAsDot(root: PsddNode, file: File, labels: Map[Int,String] = alphabet): Unit = {
    val childrenBeforeParentNodes = decisionChildrenBeforeParents(root)
    val pw = new PrintWriter(file)
    pw.write(
      """digraph sdd {

overlap=false

      """)

    for ((node,i)<-childrenBeforeParentNodes.zipWithIndex) {
      node.vtree match {
        case vtree: VtreeInternalVar =>
          if (node.elements.size==1){
            val uNode = node.elements.head.prime.asInstanceOf[PsddLiteral]
            node.baggage = (if (uNode.pos) "" else "&not;") + labels.getOrElse(uNode.v, uNode.v)
          }
          else {
            val v = vtree.v
            val pr = node.elements.find(_.prime.asInstanceOf[PsddLiteral].pos).get.theta
            node.baggage = labels.getOrElse(v, v)+{":"+Math.round(100*Math.exp(pr))/100.0}
          }

        case vtree: VtreeInternal =>
          pw.write("n"+node.index+" [label= \""+vtree.index+"\",style=filled,fillcolor=gray95,shape=circle,height=.25,width=.25];\n")
          var j=0
          node.elements.foreach{ el =>
            var childEdges = ""
            val textL = if (el.prime.baggage!=null) el.prime.baggage.asInstanceOf[String] else {
              childEdges += "n"+node.index+"e"+j+":L:c->n"+el.prime.index+" [arrowsize=.50,tailclip=false,arrowtail=dot,dir=both];\n"
              ""
            }
            val textR = if (el.sub.baggage!=null) el.sub.baggage.asInstanceOf[String] else {
              childEdges += "n"+node.index+"e"+j+":R:c->n"+el.sub.index+" [arrowsize=.50,tailclip=false,arrowtail=dot,dir=both];\n"
              ""
            }
            pw.write("n"+node.index+"e"+j+"""
    [label= "<L>"""+textL+"|<R>"+textR+"""",
    shape=record,
    fontsize=20,
    fontname="Times-Italic",
    fillcolor=white,
    style=filled,
    fixedsize=false,
    height=.30];

                                       """)
            pw.write("n"+node.index+"->n"+node.index+"e"+j+" [arrowsize=.50, label=\""+Math.round(100*Math.exp(el.theta))/100.0+"\"];\n")
            pw.write(childEdges)
            j+=1
          }
          pw.write("\n")
      }
    }

    //closing
    pw.write("\n}")
    pw.flush()
    pw.close()

    childrenBeforeParentNodes.foreach{_.baggage=null}
  }

  def saveAsDot2(root: PsddNode, file: File, labels: Map[Int,String] = alphabet): Unit = {
    val childrenBeforeParentNodes = childrenBeforeParents(root)
    val pw = new PrintWriter(file)
    pw.write(
      """digraph sdd {

overlap=false

      """)

    for ((node,i)<-childrenBeforeParentNodes.zipWithIndex) {
      node match{
        case node: PsddLiteral => node.baggage = (if (node.pos) "" else "&not;") + labels.getOrElse(node.v, node.v)
        case node: PsddTrue => node.baggage = "&#8868;"
        case node: PsddDecision =>
          pw.write("n"+node.index+" [label= \""+node.vtree.index+"\",style=filled,fillcolor=gray95,shape=circle,height=.25,width=.25];\n")
          var j=0
          node.elements.foreach{ el =>
            var childEdges = ""
            val textL = if (el.prime.baggage!=null) el.prime.baggage.asInstanceOf[String] else {
              childEdges += "n"+node.index+"e"+j+":L:c->n"+el.prime.index+" [arrowsize=.50,tailclip=false,arrowtail=dot,dir=both];\n"
              ""
            }
            val textR = if (el.sub.baggage!=null) el.sub.baggage.asInstanceOf[String] else {
              childEdges += "n"+node.index+"e"+j+":R:c->n"+el.sub.index+" [arrowsize=.50,tailclip=false,arrowtail=dot,dir=both];\n"
              ""
            }
            pw.write("n"+node.index+"e"+j+"""
    [label= "<L>"""+textL+"|<R>"+textR+"""",
    shape=record,
    fontsize=20,
    fontname="Times-Italic",
    fillcolor=white,
    style=filled,
    fixedsize=false,
    height=.30];

                                       """)
            pw.write("n"+node.index+"->n"+node.index+"e"+j+" [arrowsize=.50, label=\""+Math.round(100*Math.exp(el.theta))/100.0+"\"];\n")
            pw.write(childEdges)
            j+=1
          }
          pw.write("\n")
      }
    }

    //closing
    pw.write("\n}")
    pw.flush()
    pw.close()

    childrenBeforeParentNodes.foreach{_.baggage=null}
  }

}
