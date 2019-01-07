package algo

import java.io.File

import junit.framework.TestCase
import operations.PsddManager
import sdd.{Sdd, VtreeType, Vtree, SddManager}
import structure.{DataSets, Data, VtreeNode}
import util.Log

class ParameterCalculatorTest extends TestCase{

  val vtree = VtreeNode.balanced(Array(1,2,3,4))
  val sddMgr = new SddManager(new Vtree(Array(1l,2l,3l,4l), VtreeType.Balanced))
  sddMgr.useAutoGcMin(false)
  val psddMgr = new PsddManager(sddMgr, true)
  val dataSet = Data.readFromFile(new File("src/test/resources/loop.data"))
  val data = new DataSets(dataSet, dataSet, dataSet)

  def testParameterCalculatorWithoutSmoothing() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, ParameterCalculatorWithoutSmoothing)

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(10.0/19.0))
    assert(elNA.theta==math.log(9.0/19.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(8.0/19.0))
    assert(elNB.theta==math.log(11.0/19.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }


  def testLaplaceParameterCalculator() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, new LaplaceParameterCalculator(1))

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(11.0/21.0))
    assert(elNA.theta==math.log(10.0/21.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(9.0/21.0))
    assert(elNB.theta==math.log(12.0/21.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }



  def testMEstimateParameterCalculator() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, new MEstimateParameterCalculator(1))

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(10.5/20.0))
    assert(elNA.theta==math.log(9.5/20.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(8.5/20.0))
    assert(elNB.theta==math.log(11.5/20.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }

  def testModelCountParameterCalculator() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, new ModelCountParameterCalculator(1))

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(18.0/35.0))
    assert(elNA.theta==math.log(17.0/35.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(9.0/21.0))
    assert(elNB.theta==math.log(12.0/21.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }


  def testMCLaplaceParameterCalculator() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, new MCLaplaceParameterCalculator(1))

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(11.0/21.0))
    assert(elNA.theta==math.log(10.0/21.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(9.0/21.0))
    assert(elNB.theta==math.log(12.0/21.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }

  def testMCMEstimateParameterCalculator() {
    val root = psddMgr.readPsdd(new File("src/test/resources/loop.psdd"),vtree,data, new MCMEstimateParameterCalculator(1))

    val elA =root.elements.find(_.formula == new Sdd(1,sddMgr)).get
    val elNA =root.elements.find(_.formula == new Sdd(-1,sddMgr)).get
    assert(elA.theta==math.log(10.5/20.0))
    assert(elNA.theta==math.log(9.5/20.0))
    val elB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(2,sddMgr)).get
    val elNB = elA.prime.elements.head.sub.elements.find(_.formula == new Sdd(-2,sddMgr)).get
    assert(elB.theta==math.log(8.5/20.0))
    assert(elNB.theta==math.log(11.5/20.0))
    assert(elA.sub.elements.head.theta==Log.one)
  }

}
