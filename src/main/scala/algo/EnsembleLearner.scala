package algo

import java.io.{File, PrintWriter}

import scala.util.Random
import scala.annotation.tailrec
import spire.math._
import operations.{PsddManager, PsddOperation, PsddQueries, Minimal, MaxDepth}
import structure.{Data, DataSets, PsddDecision, VtreeNode}
import util.Util
import main._
import algo._

import sdd.{SddManager, Vtree}

/**
  *
  * The EnsembleLearner's purpose is to learn an ensemble of PSDDs from data.
  *
  */

abstract class EnsembleLearner(data: DataSets,
                               val numComponentLearners: Int,
                               outputDir: String,
                               val parameterCalculator: ParameterCalculator,
                               val scorer: OperationScorer,
                               val maxNbIterations: Int) {
  val trainData = data.train
  val trainSize = BigDecimal.decimal(trainData.weights.sum)
  val trainDataWeightsBigDecimal = trainData.weights.map(BigDecimal.decimal(_))

  val validData = data.valid
  val validSize = BigDecimal.decimal(validData.weights.sum)
  val validDataWeightsBigDecimal = validData.weights.map(BigDecimal.decimal(_))

  val testData = data.test
  val testSize = BigDecimal.decimal(testData.weights.sum)
  val testDataWeightsBigDecimal = testData.weights.map(BigDecimal.decimal(_))

  //initialize output files

  Output.init(new File(outputDir))
  val outputForMixture = new PrintWriter(new File(outputDir+"progress.csv"))
  outputForMixture.println("It;Time;TotalTime;Size;ComponentsWeights[Array];TestLl")
  outputForMixture.flush()

  0 until numComponentLearners foreach(i=>(new File(outputDir+i.toString+"/")).mkdirs())
  val outputForLearners = for (i<-0 until numComponentLearners) yield new PrintWriter(new File(outputDir+i.toString+"/progress.csv"))
  outputForLearners.foreach{x=>
    x.println("It;Time;TotalTime;Size;Op;TrainLL")
    x.flush()
  }


  def learn(psdds: IndexedSeq[PsddDecision], psddMgr: PsddManager): Unit = {}  //implement by concrete ensemble learner class
}

class SoftEM(data: DataSets,
             numComponentLearners: Int,
             outputDir: String,
             parameterCalculator: ParameterCalculator,
             scorer: OperationScorer,
             maxNbIterations: Int,
             val structureChangeIt: Int,
             val parameterLearningIt: Int) extends EnsembleLearner(data, numComponentLearners, outputDir, parameterCalculator, scorer, maxNbIterations){

  val lambdaWeight = 0.05
  val randomGenerator = new Random()

  protected def changeStructure(psddMgr:PsddManager, psdd:PsddDecision, splitOperationFinder:SplitOperationFinder, cloneOperationFinder: CloneOperationFinder, nbOfTrainingSamples:Double):PsddOperation = {

    val levelOnWhichOperationsArePerformed = randomGenerator.nextInt(psddMgr.totalVtreeLevels+1)
    val nodesWithParents = PsddQueries.decisionNodesWithParents(psdd).filter{case (node,_)=>node.vtree.level==levelOnWhichOperationsArePerformed}

    //find split operations
    val splitOps = nodesWithParents.flatMap { case (node, _) => node.elements.map { element => splitOperationFinder.find(node, element) } }.filterNot(_._1 == null)
    val bestSplitOp = if (!splitOps.isEmpty) splitOps.maxBy(_._2) else null

    //find clone operations
    val cloneOps = nodesWithParents.map { case (node, parents) => cloneOperationFinder.find(node, parents) }.filterNot(_._1 == null)
    val bestCloneOp = if (!cloneOps.isEmpty) cloneOps.maxBy(_._2) else null

    //find best Op
    val candidates = Array(bestSplitOp,bestCloneOp).filterNot(_==null)
    val bestOp = if (candidates.length > 0) candidates.maxBy(_._2)._1 else null

    //execute the best op
    if (bestOp != null) {
      val executeResult = bestOp.execute()
      bestOp
    }else{
      null
    }
  }

  protected def buildMixture(psdds:IndexedSeq[PsddDecision],componentsWeights:IndexedSeq[BigDecimal],output:PrintWriter): Unit ={
    //combine to form an ensembler learner
    val mixtureTestLlonEachExample = testData.backend.map(example=> (psdds.map(PsddQueries.bigDecimalProb(_,example)),componentsWeights).zipped.map(_*_).sum)
    val testLl = (mixtureTestLlonEachExample.map(x=>spire.math.log(x)/testSize),testDataWeightsBigDecimal).zipped.map(_*_).sum
    val totalSize = psdds.map(psdd=>PsddQueries.size(psdd)).sum

    val time = (System.nanoTime()-currentMixtureTime)*math.pow(10,-9)
    mixtureTimer+= time
    currentMixtureTime = System.nanoTime()

    output.println(Array(getIt(),time,mixtureTimer,totalSize,componentsWeights.map("%1.2f".format(_)).mkString(";"),testLl).mkString(";"))
    output.flush()
  }

  protected def updateDataInPsdd(psddMgr:PsddManager, psdd:PsddDecision, trainData: Data): Unit = {
    psddMgr.distributeData(psdd, new DataSets(trainData, validData, testData))
    psddMgr.calculateParameters(psdd, parameterCalculator, psdd)
  }

  protected def reportComponent(output:PrintWriter,psdd:PsddDecision, time: Double, totalTime:Double, op:PsddOperation): Unit = {
    val trainLl = PsddQueries.logLikelihood(psdd, "train")/psdd.data.train.total
    val size = PsddQueries.size(psdd)
    output.println(Array(getIt(), time, totalTime, size, op, trainLl).mkString(";"))
    output.flush()
  }


  protected def calculateComponentsWeights(clusterData:IndexedSeq[Data]): IndexedSeq[BigDecimal] = {
    val samplesTotalNumber = BigDecimal.decimal(clusterData.map(_.weights.sum).sum)
    for (i<-0 until numComponentLearners) yield BigDecimal.decimal(clusterData(i).weights.sum)/samplesTotalNumber
  }

  var lastValidLl = BigDecimal.decimal(-1000.0)
  protected def whetherImprovedOnValidLl(psdds:IndexedSeq[PsddDecision],componentsWeights:IndexedSeq[BigDecimal]): Boolean = {
    val mixtureValidLlonEachExample = validData.backend.map(example=> (psdds.map(PsddQueries.bigDecimalProb(_,example)),componentsWeights).zipped.map(_*_).sum)
    val validLl = (mixtureValidLlonEachExample.map(x=>spire.math.log(x)/validSize),validDataWeightsBigDecimal).zipped.map(_*_).sum
    val improved = validLl > lastValidLl
    lastValidLl = validLl
    return improved
  }

  var mixtureTimer = 0.0
  val learnerTimer = Array.fill[Double](numComponentLearners)(0.0)
  var currentLearnerTime = System.nanoTime()
  var currentMixtureTime = System.nanoTime()

  protected def updateTimer(learnerIndex:Int): Double ={
    val time = (System.nanoTime()-currentLearnerTime)*math.pow(10,-9)
    learnerTimer(learnerIndex) += time
    currentLearnerTime = System.nanoTime()
    return time
  }

  protected def getLearnerTotalTime(learnerIndex:Int): Double = {
    return learnerTimer(learnerIndex)
  }

  var it = 0

  protected def updateIt(): Unit = {
    it +=1
  }

  protected def getIt(): Int = it

  @tailrec
  final def kMeansClustering(samples: Array[Array[Int]], weights:Array[Double], centers: IndexedSeq[Array[Double]] = null,nbOfIterations:Int = 0): Array[Int] = {
    val c = if (centers == null) {for (i<-0 until numComponentLearners) yield samples(randomGenerator.nextInt(samples.length)).map(_.toDouble)} else centers

    //reassign points to closest clusters
    val pos = samples.map{sample=>
      val distance = c.map(Util.euclideanDistance(sample,_))
      distance.indexOf(distance.min)
    }

    //recalculate each cluster's center
    val newCenters = for (i<-0 until numComponentLearners) yield {
      val filteredPos = pos.zipWithIndex.filter { case (p, _) => p == i }
      if (!filteredPos.isEmpty) {
        val filteredSamples = filteredPos.map { case (_, index) => samples(index) }
        val filteredWeights = filteredPos.map { case (_, index) => weights(index) }
        val weightedSamples = (filteredSamples, filteredWeights).zipped.map { case (s, w) => s.map(_ * w) }
        weightedSamples.transpose.map(x => (x.sum / filteredWeights.sum))
      }else{
        samples(randomGenerator.nextInt(samples.length)).map(_.toDouble)
      }
    }

    if (c.corresponds(newCenters)(sampleCenterComp) || nbOfIterations>=2000) return pos else kMeansClustering(samples,weights,newCenters,nbOfIterations+1)
  }

  val sampleCenterComp = (a:Array[Double],b:Array[Double]) => if (a.size != b.size) false else (a,b).zipped.forall{case(aa,bb)=> Math.abs(aa-bb)<0.0001}

  override def learn(psdds: IndexedSeq[PsddDecision], psddMgr: PsddManager): Unit = {

    val trainingSamples = trainData.backend.map(Util.convertAssignmentToArray(_))

    //initialize operation finders
    val minSplitOperationFinders = for (i <- 0 until numComponentLearners) yield new SplitOperationFinder(psddMgr, Minimal, scorer, parameterCalculator, psdds(i) ,1)
    val minCloneOperationFinders = for (i <- 0 until numComponentLearners) yield new CloneOperationFinder(psddMgr, Minimal, scorer, parameterCalculator, psdds(i), 3)

    //cluster initial training samples
    var pos = kMeansClustering(trainingSamples, trainData.weights)
    var trainingSampleClusters = clusterTrainingSamplesAccordingToPos(trainingSamples,trainData.weights,pos)
    0 until numComponentLearners foreach { i => updateDataInPsdd(psddMgr, psdds(i), trainingSampleClusters(i)) }


    //report scores with initial psdds
    0 until numComponentLearners foreach {i =>
      val time = updateTimer(i)
      reportComponent(outputForLearners(i), psdds(i), time, getLearnerTotalTime((i)), null)
    }
    buildMixture(psdds, calculateComponentsWeights(trainingSampleClusters), outputForMixture)
    updateIt()

    var it = 0
    while (it < maxNbIterations){
      //update structure
      outputForMixture.println("Strucutre Changed")
      outputForMixture.flush()
      0 until numComponentLearners foreach { i => updateDataInPsdd(psddMgr, psdds(i), trainingSampleClusters(i)) }
      val structureChangeUntilItNumber = getIt()+structureChangeIt

      do{
        0 until numComponentLearners foreach {i =>
          val op = changeStructure(psddMgr, psdds(i), minSplitOperationFinders(i), minCloneOperationFinders(i), trainingSampleClusters(i).weights.sum)
          val time = updateTimer(i)
          reportComponent(outputForLearners(i), psdds(i), time, getLearnerTotalTime((i)), op)
        }
        buildMixture(psdds, calculateComponentsWeights(trainingSampleClusters), outputForMixture)
        updateIt()
      }while(getIt()<structureChangeUntilItNumber && whetherImprovedOnValidLl(psdds,calculateComponentsWeights(trainingSampleClusters)))

      //optimize parameters
      outputForMixture.println("Strucutre Fixed")
      outputForMixture.flush()
      val parameterOptimizeUntilItNumber = getIt()+parameterLearningIt
      do{
        trainingSampleClusters = redistributeTrainingData(trainData.backend, trainData.weights, psdds)
        0 until numComponentLearners foreach { i =>
          updateDataInPsdd(psddMgr, psdds(i), trainingSampleClusters(i))
          val time = updateTimer(i)
          reportComponent(outputForLearners(i), psdds(i), time, getLearnerTotalTime((i)), null)
        }
        buildMixture(psdds, calculateComponentsWeights(trainingSampleClusters), outputForMixture)
        updateIt()
      }while(getIt()<parameterOptimizeUntilItNumber && whetherImprovedOnValidLl(psdds,calculateComponentsWeights(trainingSampleClusters)))
      trainingSampleClusters = redistributeTrainingData(trainData.backend, trainData.weights, psdds)
    }

    outputForMixture.close()
    outputForLearners.foreach(_.close())

    it = it + 1
  }

  private def clusterTrainingSamplesAccordingToPos(samples:Array[Array[Int]],weights:Array[Double],pos:Array[Int]):IndexedSeq[Data] = {
    require(samples.length == pos.length)
    require(pos.length == pos.length)
    for (learnerIndex<- 0 until numComponentLearners) yield {
      val softWeights = pos.zipWithIndex.map{case(p,i)=> if (p==learnerIndex) weights(i)-(numComponentLearners-1)*lambdaWeight else lambdaWeight}
      Data.readFromArray(samples, softWeights)
    }
  }

  private def redistributeTrainingData(samples:Array[Map[Int,Boolean]], weights:Array[Double], psdds:IndexedSeq[PsddDecision]): Array[Data] = {
    val redistributedWeights = (samples,weights).zipped.map{case(sample,weight)=>
      val probs = psdds.map(PsddQueries.bigDecimalProb(_,sample)).toArray
      probs.map(p=>(p/probs.sum*weight).toDouble)
    }
    redistributedWeights.transpose.map(w=> Data.readDataAndWeights((samples,w).zipped.toArray))
  }
}



