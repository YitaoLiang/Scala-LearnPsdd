package algo

import operations.Result


abstract class OperationScorer {
  def score(result: Result): Double
}

object DllScorer extends OperationScorer {
  override def score(results: Result): Double = results.dll
}


object DllPerDsizeScorer extends OperationScorer {
  override def score(results: Result): Double = results.dll/results.dSize
}
