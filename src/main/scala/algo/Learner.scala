package algo
import main._
import operations._
import structure.PsddDecision
import util.Util
import Math._

/**
  *
  * The Learner's purpose is to learn a PSDD from data.
  * This abstract class provides some general functionality such as:
  *     - the initialization for the learning
  *     - reporting
  *     - manage saving the psdds given the saving strategy
  *
 */
abstract class Learner(operationFinders: Array[SpecificOperationQueue[_ <: PsddOperation, _<:Object]],
                       parameterCalculator: ParameterCalculator,
                       saveFrequency: SaveFrequency) {

  def header(): Unit = {
    Output.writeln(Array("Cycle", "It", "Time", "TotalTime", "Size", "Operation", "TrainLL", "ValidLL", "TestLL").mkString(";"), "progress")
  }

  var totTime = 0.0

  /**
    * report the progress
    * @param operation the operation that gave the progress
    */
  def report(operation: PsddOperation): Unit = {
    time1 = System.nanoTime()
    var time = (time1 - time0) * nano
    totTime += time
    Output.writeln(Array(cycle, it, time, totTime, size, operation, trainLl / trainDataSize, validLl / validDataSize, testLl / testDataSize).mkString(";"), "progress")
    time0 = System.nanoTime()
  }

  /**
    * Extend the given PSDD by learning from data.
    *
    * the abstract class only initializes. The rest of the learning is different for every instantiation
    * @param root root
    * @param psddManager manager
    */
  def learn(root: PsddDecision, psddManager: PsddManager): Unit = {
    this.root = root
    Output.addWriter("progress", "progress.csv")
    totTime = 0.0
    time0 = System.nanoTime()

    trainDataSize = root.data.train.total
    validDataSize = root.data.valid.total
    testDataSize = root.data.test.total

    trainLl = PsddQueries.logLikelihood(root, "train")
    validLl = PsddQueries.logLikelihood(root, "valid")
    testLl = PsddQueries.logLikelihood(root, "test")
    size = PsddQueries.size(root)

    resetCycle()
    resetIt()

    header()

    report(null)
    savePsdd()
    initializeDebug()
  }

  /**
    *update all the lls given the execution result
    * @param res res
    */
  protected def updateLls(res: ExecutionResult): Unit = {
//    trainLl += res.dll
//    validLl += res.validDll
//    testLl += res.testDll

    // more robust in case of rounding errors (or others)
    trainLl = PsddQueries.logLikelihood(root, "train")
    validLl = PsddQueries.logLikelihood(root, "valid")
    testLl = PsddQueries.logLikelihood(root, "test")
    val newSize = PsddQueries.size(root)
    println("won edges:" + (size+res.dSize - newSize))
    size = newSize

  }

  // variables
  var root: PsddDecision = _

  var trainLl = 0.0
  var validLl = 0.0
  var oldValidLl = 0.0
  var testLl = 0.0
  var size = 0.0

  var bestSavedValidLl = Double.NegativeInfinity

  var trainDataSize = 0.0
  var validDataSize = 0.0
  var testDataSize = 0.0

  var cycle = 0
  var it = 0

  var time0 = 0.0
  var time1 = 0.0
  val nano: Double = math.pow(10, -9)

  /**
    * Save the current psdd
    * Saving is only attempted every k cycles.
    * There are different saving strategies:
    *   saving all or only keeping the best according to the validation set log likelihood (more space efficient)
    *
    * As a side effect, the characteristics of the best model are always written away.
    *
    * For debugging purpose, the last psdd is also always saved.
    */
  def savePsdd(): Unit = {

    val validLlImprovement = validLl > bestSavedValidLl

    val saveCurrentPsdd = saveFrequency match {
      case All(k) => it%k==0
      case Best(k) => it % k == 0 && validLlImprovement
    }


    if (saveCurrentPsdd) {
      if (validLlImprovement) {
        bestSavedValidLl = validLl

        // write out the characteristics of the best model.
        Output.addWriter("best")
        Output.writeln("cycle,it,size,trainLl, validLl, testLl", "best")
        Output.writeln(Array[Any](cycle, it, size, trainLl / trainDataSize, validLl / validDataSize, testLl / testDataSize).mkString(","), "best")
        Output.closeWriter("best")
      }


      // delete files if needed
      saveFrequency match {
        case All(_) =>
        case Best(k) => Output.modelFolder.listFiles().filterNot(_.isDirectory).foreach(_.delete()) // delete all other models
      }

      Output.savePsdds(root, cycle + "-" + it, asPsdd = true, asDot = true)

    }

    // always keep last model too (for debug purpose)
    val previousLast = Output.modelFolder.listFiles().filter(_.getName.contains("last")) // delete the previous last model
    Output.savePsdds(root,"last_"+cycle + "-" + it, asPsdd = true, asDot = true)
    previousLast.foreach(_.delete())
  }

  protected def initializeDebug(): Unit ={
    Debug.addWriter("lls","llComparison.csv",3)
    Debug.writeln("lls",3,"cycle,it,executionDll, simulationDll, executionTrainLl, simulationTrainLl,realTrainLl, executionValidLl, realValidLl, executionTestLl, realTestLl")
    Debug.writeln("lls",3,Array(0,0,0.0, 0.0, trainLl, trainLl,trainLl, validLl, validLl, testLl, testLl).mkString(","))

  }

  protected def writeDebug(op: PsddOperation, res: ExecutionResult): Unit = {
    Debug.writeln("lls", 3, Array(cycle, it, res.dll, op.dll, trainLl, trainLl - res.dll + op.dll, PsddQueries.logLikelihood(root, "train"),validLl, PsddQueries.logLikelihood(root, "valid"), testLl, PsddQueries.logLikelihood(root, "test")).mkString(","))
  }

  protected def resetIt(): Unit ={
    it = 0
    Output.addCounter("it")
  }
  protected def incrementIt(): Unit ={
    it += 1
    Output.increment("it")
  }

  protected def resetCycle(): Unit ={
    cycle = 0
    Output.addCounter("cycle")
  }
  protected def incrementCycle(): Unit ={
    cycle += 1
    Output.increment("cycle")
  }


  /**
    * If assertions are on, this method executes some sanity checks.
    * @param op op
    * @param res res
    * @return
    */
  protected def operationResultsCorrect(op: PsddOperation, res: ExecutionResult): Boolean = {

    assert(PsddQueries.isValid(root), "invalid psdd")

//    assert(operationFinders.head.operationCompletionType.isInstanceOf[MaxEdges] || res.dSize == op.dSize, res.dSize + " <=> " + op.dSize)
//
//    assert(operationFinders.head.operationCompletionType.isInstanceOf[MaxEdges] || Util.isEqual(res.dll, op.dll))
    assert(Util.isEqual(trainLl, PsddQueries.logLikelihood(root, "train")), (trainLl, PsddQueries.logLikelihood(root, "train"), root.data.train.weightedIterator.map { case (example, weight) => PsddQueries.logProb(root, example) * weight }.sum ))
    assert(Util.isEqual(testLl, PsddQueries.logLikelihood(root, "test")),(testLl, PsddQueries.logLikelihood(root, "test"),root.data.test.weightedIterator.map { case (example, weight) => PsddQueries.logProb(root, example) * weight }.sum ))
    assert(Util.isEqual(validLl, PsddQueries.logLikelihood(root, "valid")),(validLl, PsddQueries.logLikelihood(root, "valid"),root.data.valid.weightedIterator.map { case (example, weight) => PsddQueries.logProb(root, example) * weight }.sum))

//    assert(op.dll >= 0)

    true
  }

}


/**
  * The Search Learner's purpose is to learn a PSDD from data by greedily searching
  * for the best operations to change the structure
  *
  * @param operationQueues opFinder
  * @param parameterCalculator paramCalc
  * @param maxNbIterations nb of iterations
  * @param saveFrequency frequency of saving
  */
class SearchLearner(operationQueues: Array[SpecificOperationQueue[_ <: PsddOperation, _ <:Object]],
                    parameterCalculator: ParameterCalculator,
                    maxNbIterations: Int,
                    saveFrequency: SaveFrequency) extends Learner(operationQueues, parameterCalculator, saveFrequency) {

  override def learn(root: PsddDecision, psddManager: PsddManager): Unit = {
    super.learn(root, psddManager)

    val structureSearcher = new OperationQueue(_=>true, true, false, operationQueues)
    structureSearcher.initialize(root, psddManager)
    

    while(structureSearcher.hasNext && it < maxNbIterations && abs(oldValidLl - validLl)>0.00001 ) {

      incrementIt()

      val op: PsddOperation = structureSearcher.next

      val res: ExecutionResult = op.execute()
      oldValidLl = validLl
      updateLls(res)

      report(op)
      writeDebug(op, res)
      savePsdd()

      assert(operationResultsCorrect(op, res))

      structureSearcher.update(res)

    }
  }
}


abstract class LevelLearner(operationQueues: Array[SpecificOperationQueue[_<:PsddOperation, _<:Object]],
                            parameterCalculator: ParameterCalculator,
                            maxNbIterationsPerVtreeNode: Int,
                            keepSplitting:Boolean,
                            keepCloning: Boolean,
                            saveFrequency: SaveFrequency) extends Learner(operationQueues, parameterCalculator, saveFrequency) {

  def firstLevel(root: PsddDecision): Int
  def lastLevel(root: PsddDecision): Int
  val increment: Int

  override def learn(root: PsddDecision, psddManager: PsddManager): Unit = {
    super.learn(root, psddManager)


    for (level <- Range(firstLevel(root), lastLevel(root)+increment,increment)){
      incrementCycle()
      resetIt()

      savePsdd()

      Debug.writeln("initialize structure searcher...")
      val structureSearcher = new OperationQueue(_.vtree.level==level,keepCloning,!keepSplitting, operationQueues)
      structureSearcher.initialize(root, psddManager)

      val a = maxNbIterationsPerVtreeNode * root.vtree.parentsBeforeChildren().count(_.level==level)
      val maxNbIterationsPerLevel = if (a<0) Int.MaxValue else a

      while(structureSearcher.hasNext && it < maxNbIterationsPerLevel){
        incrementIt()

        val op: PsddOperation = structureSearcher.next
        Debug.writeln("it "+cycle+"-"+it+": execute operation "+op)
        val res = op.execute()

        updateLls(res)

        Debug.writeln("it "+cycle+"-"+it+": report")
        report(op)
        writeDebug(op, res)
        savePsdd()

        assert(operationResultsCorrect(op, res))

        Debug.writeln("it "+cycle+"-"+it+": update operations")
        structureSearcher.update(res)
      }
    }
  }


}

class BottomUpLearner(operationQueues: Array[SpecificOperationQueue[_<:PsddOperation, _<:Object]],
                      parameterCalculator: ParameterCalculator,
                      maxNbIterationsPerVtreeNode: Int,
                      keepSplitting:Boolean,
                      keepCloning: Boolean,
                      saveFrequency: SaveFrequency) extends LevelLearner(operationQueues, parameterCalculator, maxNbIterationsPerVtreeNode, keepSplitting, keepCloning, saveFrequency) {

  override def firstLevel(root: PsddDecision): Int = root.vtree.maxLevel

  override def lastLevel(root: PsddDecision): Int = 0

  override val increment: Int = -1

}

class TopDownLearner(operationQueues: Array[SpecificOperationQueue[_<:PsddOperation, _<:Object]],
                     parameterCalculator: ParameterCalculator,
                     maxNbIterationsPerVtreeNode: Int,
                     keepSplitting:Boolean,
                     keepCloning: Boolean,
                     saveFrequency: SaveFrequency) extends LevelLearner(operationQueues, parameterCalculator, maxNbIterationsPerVtreeNode, keepSplitting, keepCloning, saveFrequency) {

  override def firstLevel(root: PsddDecision): Int = 0

  override def lastLevel(root: PsddDecision): Int = root.vtree.maxLevel

  override val increment: Int = 1

}
