package util

import structure.Data

/**
  *
  * Helper class for calculating pairwise mutual information
  * Used in the vtree learner
  *
  */
object MutualInformation {

  def setEnt(x: Array[Int], pairwiseWeights: Array[Array[Double]], v2i: Map[Int, Int]): Double = {
    var weightsTot = 0
    var sum = 0.0
    for (xi <- x; xj<-x){
      weightsTot += 1
      sum +=  pairwiseWeights(v2i(xi))(v2i(xj))
    }
    sum/weightsTot
  }


  def setMi(x: Array[Int], y: Array[Int], pairwiseWeights: Array[Array[Double]], v2i: Map[Int,Int]): Double = {
    var weightsTot = 0
    var sum = 0.0
    for (xi <- x; yi<-y){
      weightsTot += 1
      sum +=  pairwiseWeights(v2i(xi))(v2i(yi))
    }
    sum/weightsTot
  }

  def pairwiseMutualInformation(data: Data) = pairwiseCalculations(data, miCalc = true, entCalc = false)._1
  def pairwiseEnt(data: Data) = pairwiseCalculations(data, miCalc = false, entCalc = true)._2
  def pairwiseMutualInformationEnt(data: Data) = pairwiseCalculations(data, miCalc = true, entCalc = false)


  def pairwiseCalculations(data: Data, miCalc: Boolean, entCalc: Boolean): (Array[Array[Double]],Array[Array[Double]]) = {

    // initialize
    val vars = data.vars
    val probs = vars.indices.map{i => val v = vars(i); val p = data.filter(_(v)).total/data.total; Array(1-p,p)}.toArray
    val varCombos = vars.indices.combinations(2).map(a =>if (a(0)<a(1)) (a(0),a(1)) else (a(1),a(0))).toList

    val pairwiseProbs = Array.fill[Array[Array[Array[Double]]]](vars.length)(Array.fill[Array[Array[Double]]](2)(Array.fill[Array[Double]](vars.length)(Array.fill[Double](2)(0.0))))


    //counts
    for ((assignment,weight)<- data.weightedIterator) {
      for ((i, j) <- varCombos) {
        pairwiseProbs(i)(if (assignment(vars(i))) 1 else 0)(j)(if (assignment(vars(j))) 1 else 0) += weight
      }
    }

    //normalize
    for ((i,j) <- varCombos; iv <-Array(0,1); jv <- Array(0,1)){
      pairwiseProbs(i)(iv)(j)(jv)/=data.total
    }


    val mi: Array[Array[Double]] = if (!miCalc) null else {
      val mi = Array.fill[Array[Double]](vars.length)(Array.fill[Double](vars.length)(0.0))


      for ((i, j) <- varCombos) {
        mi(i)(j) = Array((1, 1), (1, 0), (0, 1), (0, 0)).foldLeft(0.0) { case (s, (iv, jv)) =>
          s + (if (pairwiseProbs(i)(iv)(j)(jv) == 0.0) 0.0 else pairwiseProbs(i)(iv)(j)(jv) * Math.log(pairwiseProbs(i)(iv)(j)(jv) / (probs(i)(iv) * probs(j)(jv))))
        }
        mi(j)(i) = mi(i)(j)
      }
      mi
    }

    val ent: Array[Array[Double]] = if (!entCalc) null else {
      val ent = Array.fill[Array[Double]](vars.length)(Array.fill[Double](vars.length)(0.0))


      for ((i, j) <- varCombos) {
        ent(i)(j) = Array((1, 1), (1, 0), (0, 1), (0, 0)).foldLeft(0.0) { case (s, (iv, jv)) =>
          s + (if (pairwiseProbs(i)(iv)(j)(jv) == 0.0) 0.0 else - pairwiseProbs(i)(iv)(j)(jv) * Math.log(pairwiseProbs(i)(iv)(j)(jv)))
        }
        ent(j)(i) = ent(i)(j)
      }
      ent
    }

    (mi,ent)
  }

}
