package operations

import java.io.File

import algo.{ParameterCalculatorWithoutSmoothing, MEstimateParameterCalculator}
import junit.framework.TestCase
import sdd.{Sdd, VtreeType, Vtree, SddManager}
import structure._
import util.{PsddComparator, Log}

class PsddManagerTest extends TestCase{

  def testIndependentVariablesPsdd(): Unit ={
    val vtree = VtreeNode.balanced(Array(1,2,3,4))
    val sddMgr = new SddManager(new Vtree(Array(1l,2l,3l,4l), VtreeType.Balanced))
    sddMgr.useAutoGcMin(false)
    val psddMgr = new PsddManager(sddMgr, true)
    val dataSet = Data(Array(), Array(), Array(1,2,3,4))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val root = psddMgr.newPsdd(vtree, data, new MEstimateParameterCalculator(1))


    assert(root.isInstanceOf[PsddDecision])
    assert(root.elements.size==1)
    assert(root.elements.head.prime.isInstanceOf[PsddDecision])
    assert(root.elements.head.prime.elements.size==1)
    assert(root.elements.head.prime.elements.head.sub.isInstanceOf[PsddDecision])
    assert(root.elements.head.prime.elements.head.sub.elements.size==2)
    assert(root.elements.head.prime.elements.head.sub.elements.exists{case PsddElement(prime,_,_,_,_) =>
      prime.isInstanceOf[PsddLiteral] && prime.asInstanceOf[PsddLiteral].literal == 2
    })
    assert(root.elements.head.prime.elements.head.sub.elements.exists{case PsddElement(prime,_,_,_,_) =>
      prime.isInstanceOf[PsddLiteral] && prime.asInstanceOf[PsddLiteral].literal == -2
    })
    var el = root.elements.head.prime.elements.head.sub.elements.find{case PsddElement(prime,_,_,_,_) =>
      prime.isInstanceOf[PsddLiteral] && prime.asInstanceOf[PsddLiteral].literal == 2
    }.get
    assert(el.sub.isInstanceOf[PsddTrue])
    assert(el.theta==math.log(0.5))
    assert(el.data.train.isEmpty)
    assert(el.formula.getLiteral==2l)

    el = root.elements.head.prime.elements.head.sub.elements.find{case PsddElement(prime,_,_,_,_) =>
      prime.isInstanceOf[PsddLiteral] && prime.asInstanceOf[PsddLiteral].literal == -2
    }.get
    assert(el.sub.isInstanceOf[PsddTrue])
    assert(el.theta==math.log(0.5))
    assert(el.data.train.isEmpty)
    assert(el.formula.getLiteral== -2l)
  }

  def testReadSdd(): Unit = {
    val vtree = VtreeNode.balanced(Array(1,2))
    val sddMgr = new SddManager(new Vtree(Array(1l,2l), VtreeType.Balanced))
    sddMgr.useAutoGcMin(false)
    val sdd = Sdd.read("src/test/resources/simple.sdd", sddMgr)
    val psddMgr = new PsddManager(sddMgr, true)
    val dataSet = Data(Array(), Array(), Array(1,2))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val root = psddMgr.readPsddFromSdd(new File("src/test/resources/simple.sdd"), vtree, data, new MEstimateParameterCalculator(1))

    assert(sdd.equals(root.formula))
    assert(root.elements.size==2)
    assert(root.elements.head.theta==math.log(0.5))

    val n1and2 = root.elements.find(_.formula==new Sdd(-1,sddMgr).conjoin(new Sdd(2, sddMgr))).get
    val one = root.elements.find(_.formula==new Sdd(1,sddMgr)).get

    assert(n1and2.prime.elements.size==1)
    assert(n1and2.prime.elements.head.theta==Log.one)
    assert(n1and2.prime.elements.head.prime.asInstanceOf[PsddLiteral].literal== -1)
    assert(n1and2.prime.elements.head.sub.isInstanceOf[PsddTrue])
    assert(one.sub.elements.size == 2)
    assert(one.sub.elements.head.theta == math.log(0.5))
    assert(one.sub.elements.head.prime.asInstanceOf[PsddLiteral].v== 2)
  }

  def testTrueSdd(): Unit = {
    val vtree = VtreeNode.balanced(Array(1,2))
    val sddMgr = new SddManager(new Vtree(Array(1l,2l), VtreeType.Balanced))
    sddMgr.useAutoGcMin(false)
    val sdd = new Sdd(true, sddMgr)
    sdd.save("src/test/resources/true.sdd")
    val psddMgr = new PsddManager(sddMgr, true)
    val dataSet = Data(Array(), Array(), Array(1,2,3,4))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val root = psddMgr.readPsddFromSdd(new File("src/test/resources/true.sdd"), vtree, data, new MEstimateParameterCalculator(1))

    assert(sdd.equals(root.formula))
    assert(root.elements.size==1)
    assert(root.elements.head.theta==Log.one)

    assert(root.elements.head.prime.elements.size==2)
    assert(root.elements.head.prime.elements.head.theta==math.log(0.5))
    assert(root.elements.head.prime.elements.head.prime.asInstanceOf[PsddLiteral].v==1)
    assert(root.elements.head.prime.elements.head.sub.isInstanceOf[PsddTrue])

    assert(root.elements.head.sub.elements.size==2)
    assert(root.elements.head.sub.elements.head.theta==math.log(0.5))
    assert(root.elements.head.sub.elements.head.prime.asInstanceOf[PsddLiteral].v==2)
    assert(root.elements.head.sub.elements.head.sub.isInstanceOf[PsddTrue])
  }


  def testReadPsdd(): Unit = {
    val vtree = VtreeNode.balanced(Array(1,2))
    val sddMgr = new SddManager(new Vtree(Array(1l,2l), VtreeType.Balanced))
    sddMgr.useAutoGcMin(false)
    val sdd = Sdd.read("src/test/resources/simple.sdd", sddMgr)
    val psddMgr = new PsddManager(sddMgr, true)
    val dataSet = Data(Array(), Array(), Array(1,2,3,4))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val root = psddMgr.readPsdd(new File("src/test/resources/simple.psdd"), vtree, data)

    assert(sdd.equals(root.formula))
    assert(root.elements.size==2)
    assert(root.elements.head.theta==math.log(0.5))

    val n1and2 = root.elements.find(_.formula==new Sdd(-1,sddMgr).conjoin(new Sdd(2, sddMgr))).get
    val one = root.elements.find(_.formula==new Sdd(1,sddMgr)).get

    assert(n1and2.prime.elements.size==1)
    assert(n1and2.prime.elements.head.theta==Log.one)
    assert(n1and2.prime.elements.head.prime.asInstanceOf[PsddLiteral].literal== -1)
    assert(n1and2.prime.elements.head.sub.isInstanceOf[PsddTrue])
    assert(one.sub.elements.size == 2)
    assert(one.sub.elements.head.theta == math.log(0.5))
    assert(one.sub.elements.head.prime.asInstanceOf[PsddLiteral].v== 2)
  }

  def testDistributeData(): Unit = {
    val vtree = VtreeNode.balanced(Array(1,2,3,4))
    val sddMgr = new SddManager(new Vtree(Array(1l,2l,3l,4l), VtreeType.Balanced))
    sddMgr.useAutoGcMin(false)
    val psddMgr = new PsddManager(sddMgr, true)
    val dataSet = Data.readFromFile(new File("src/test/resources/loop.data"))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data)

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.data.train.size==8.0)
    assert(elA.data.train.total==10.0)
    assert(elNA.data.train.size==8.0)
    assert(elNA.data.train.total==9.0)
    assert(elA.sub.elements.head.data.train.size==16)
    assert(elA.sub.elements.head.data.train.total==19.0)
  }

  def testMaxDepthOperations(): Unit = {

    val vtree = VtreeNode.read(new File("src/test/resources/vtree14.vtree"))
    val sddMgr = new SddManager(Vtree.read("src/test/resources/vtree14.vtree"))
    sddMgr.useAutoGcMin(false)
    val psddMgr = new PsddManager(sddMgr, cache = true)

    val dataSet = Data.readFromFile(new File("src/test/resources/data14All.train"))
    val data = new DataSets(dataSet, dataSet, dataSet)
    val parameterCalculator = ParameterCalculatorWithoutSmoothing

    val psdd = psddMgr.newPsdd(vtree.asInstanceOf[VtreeInternal], data, parameterCalculator)

    val psddMgr1 = new PsddManager(sddMgr, true)
    val psdd1 = psddMgr1.readPsdd(new File("src/test/resources/psdd14.1.psdd"), vtree, data)

    val psddMgr2 = new PsddManager(sddMgr, true)
    val psdd2 = psddMgr2.readPsdd(new File("src/test/resources/psdd14.2.psdd"), vtree, data)

    val psddMgr3 = new PsddManager(sddMgr, true)
    val psdd3 = psddMgr3.readPsdd(new File("src/test/resources/psdd14.3.psdd"), vtree, data)

    val psddMgr4 = new PsddManager(sddMgr, true)
    val psdd4 = psddMgr4.readPsdd(new File("src/test/resources/psdd14.psdd"), vtree, data)



    assert(!PsddComparator.psddEqual(psdd, psdd4, checkParameters = true, checkData = false))

    val splitNode1 = psdd.elements.head.prime.elements.head.sub.asInstanceOf[PsddDecision]
    val splitElement1 = splitNode1.elements.head
    psddMgr.executeSplit(splitNode1, splitElement1, parameterCalculator, Array(ConjunctionConstraint(Map(8->true)),ConjunctionConstraint(Map(8->false))), MaxDepth(2), psdd)

    assert(PsddComparator.psddEqual(psdd, psdd1, checkParameters = true, checkData = false))

//    val psdd = psddMgr.readPsdd(new File("src/test/resources/psdd14.psdd"), vtree, data, parameterCalculator)
    val splitNode2 = psdd.elements.head.prime.elements.head.prime.asInstanceOf[PsddDecision]
    val splitElement2 = splitNode2.elements.head
    psddMgr.executeSplit(splitNode2, splitElement2, parameterCalculator, Array(ConjunctionConstraint(Map(2->true)),ConjunctionConstraint(Map(2->false))), Complete, psdd)
//
//
    assert(PsddComparator.psddEqual(psdd, psdd2, checkParameters = true, checkData = false))

    //    val psdd = psddMgr.readPsdd(new File("src/test/resources/psdd14.psdd"), vtree, data, parameterCalculator)
    val splitNode3 = psdd.elements.head.prime.asInstanceOf[PsddDecision]
    val splitElement3 = splitNode3.elements.head

    psddMgr.executeSplit(splitNode3, splitElement3, parameterCalculator, Array(ConjunctionConstraint(Map(4->true)), ConjunctionConstraint(Map(4->false))), MaxDepth(1), psdd)

    assert(PsddComparator.psddEqual(psdd, psdd3, checkParameters = true, checkData = false))
  }

  def testCloneExecution(): Unit = {

  }

  def testSplitSimulation(): Unit ={

  }

  def testCloneSimulation(): Unit ={

  }

}
