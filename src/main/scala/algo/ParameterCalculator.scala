package algo

import structure.Data
import util.{Util, Log}

/**
  * This class calculates the parameter of an element in log space.
  *
 **/
abstract class ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double
}

object ParameterCalculatorWithoutSmoothing extends ParameterCalculator {

  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    val elementCount = elementData.total
    val nodeCount = nodeData.total
    math.log(elementCount/nodeCount)
  }
  override def toString: String = "no"
}

class LaplaceParameterCalculator(m: Double) extends ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    val elementCount = elementData.total + m
    val nodeCount = nodeData.total + m*nbOfElements
    math.log(elementCount/nodeCount)
  }
  override def toString: String = "l-"+m
}

class MEstimateParameterCalculator(m: Double) extends ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    val elementCount = elementData.total + m/nbOfElements
    val nodeCount = nodeData.total + m
    math.log(elementCount/nodeCount)
  }

  override def toString: String = "m-"+m
}

class ModelCountParameterCalculator(m: Double) extends ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    require(elementMC<=nodeMC || Util.isEqual(elementMC, nodeMC))

    val elementCount = Log.add(math.log(elementData.total),math.log(m)+elementMC)
    val nodeCount = Log.add(math.log(nodeData.total),m+nodeMC)
    val res = elementCount-nodeCount
    require(res!=Double.NaN)
    res
  }

  override def toString: String = "mc-"+m
}

class MCLaplaceParameterCalculator(m: Double) extends ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    require(elementMC<=nodeMC || Util.isEqual(elementMC, nodeMC))
    val elementCount = Log.add(math.log(elementData.total),math.log(m*nbOfElements)+elementMC-nodeMC)
    val nodeCount = math.log(nodeData.total + m*nbOfElements)
    val res = elementCount-nodeCount
    require(res!=Double.NaN)
    res
  }

  override def toString: String = "mc-l-"+m
}

class MCMEstimateParameterCalculator(m: Double) extends ParameterCalculator {
  def calculate(elementData: Data, nodeData: Data, elementMC: => Double, nodeMC: => Double, nbOfElements: Int): Double = {
    require(elementMC<=nodeMC || Util.isEqual(elementMC, nodeMC))
    require(nodeMC>Double.NegativeInfinity)
    require(elementMC>Double.NegativeInfinity)
    val elementCount = Log.add(math.log(elementData.total),math.log(m)+elementMC-nodeMC)
    val nodeCount = math.log(nodeData.total + m)
    val res = elementCount-nodeCount
    require(res!=Double.NaN)
    res
  }

  override def toString: String = "mc-m-"+m
}