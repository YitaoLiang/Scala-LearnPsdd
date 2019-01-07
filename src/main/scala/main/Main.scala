package main

import java.io.{PrintWriter, File}

import algo._
import operations._
import sdd.{SddManager, Vtree}
import structure._

import scala.util.Random


object Main {


//  Different functionalities of the code
  val learnEnsemblePsdd = "learnEnsemblePsdd"
  val learnPsdd = "learnPsdd"
  val sdd2psdd = "sdd2psdd"
  val parameterLearning = "learnParams"
  val learnVtree = "learnVtree"
  val paramSearch = "paramSearch"
  val scratch = "scratch"
  val check = "check"

//  structure learning strategies
  val bottomUp = "BU"
  val topDown = "TD"
  val search = "search"
  val learnMethods = Array(bottomUp, topDown, search)


  val commands = Array(learnMethods.map(learnPsdd+" "+_).mkString(" ","|",""), sdd2psdd, parameterLearning, learnVtree, paramSearch, check)

//datastets
  val dataSets = Array("train","valid","test")


//  Vtrees
  val vtreeOptions = Map(
    "miMetis" -> "balanced vtree by top down selection of the split that minimizes the mutual information between the two parts, using metis.",
    "miBlossom" -> "balanced vtree by bottom up matching of the pairs that maximizes the average mutual information in a pair, using blossomV.",
    "balanced-ord" -> "balanced vtree using variable order",
    "rightLinear-ord" -> "right linear vtree using variable order",
    "leftLinea-ord" -> "left linear vtree using variable order",
    "balanced-rand" -> "balanced vtree using a random order",
    "rightLinear-rand" -> "right linear vtree using a random order",
    "leftLinear-rand" -> "left linear vtree a random order",
    "pairwiseWeights" -> "balanced vtree by top down selection of the split that minimizes the mutual information between the two parts, using exhaustive search.",
    "miGreedyBU" -> "balanced vtree by bottom up matching of the pairs that maximizes the average mutual information in a pair, using greedy selection."
  )

  val defaultVtree = "miBlossom"

// structure search operations
  val clonePrefix = "clone"
  val splitPrefix = "split"
  val split = (splitPrefix+"-(\\d+)").r
  val cloneOp = (clonePrefix+"-(\\d+)").r
  val operationTypes = Seq(clonePrefix+"-<k>", splitPrefix+"-<k>")
  val defaultOperationTypes = Seq(clonePrefix+"-3", splitPrefix+"-1")

// parameter calculators (differ in smoothing type)
  val no = "no"
  val mEstimatorPrefix = "m"
  val laplacePrefix = "l"
  val modelCountPrefix = "mc"
  val parameterCalculators = Map(
    "no" -> "No smoothing",
    mEstimatorPrefix+"-<m>" -> "m-estimator smoothing",
    laplacePrefix+"-<m>" -> "laplace smoothing, weighted with m",
    modelCountPrefix+"-<m>" -> "model count as pseudo count, weighted with m",
    modelCountPrefix+"-"+mEstimatorPrefix+"-<m>" -> "m-estimator, weighted with model count",
    modelCountPrefix+"-"+laplacePrefix+"-<m>" -> "laplace smoothing, weighted with model count and m"
  )
  val defaultParameterCalculator = new LaplaceParameterCalculator(1)
  val defaultParameterCalculatorString = "l-1"

  val float = "[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?"
  val mEstimator = (mEstimatorPrefix+"-(" + float + ")").r
  val laplace = (laplacePrefix+"-(" + float + ")").r
  val mc = (modelCountPrefix+"-(" + float + ")").r
  val mcMEstimator = (modelCountPrefix+"-"+mEstimatorPrefix+"-(" + float + ")").r
  val mcLaplace = (modelCountPrefix+"-"+laplacePrefix+"-(" + float + ")").r

  implicit val paramCalcRead: scopt.Read[ParameterCalculator] = scopt.Read.reads{
    case `no` => ParameterCalculatorWithoutSmoothing
    case mEstimator(m) => new MEstimateParameterCalculator(m.toDouble)
    case laplace(m) => new LaplaceParameterCalculator(m.toDouble)
    case mc(m) => new ModelCountParameterCalculator(m.toDouble)
    case mcMEstimator(m) => new MCMEstimateParameterCalculator(m.toDouble)
    case mcLaplace(m) => new MCLaplaceParameterCalculator(m.toDouble)
  }

//  scorers
  val dllScorer = "dll"
  val dllPerSizeScorer = "dll/ds"
  val scorers = Array(dllScorer, dllPerSizeScorer)
  val defaultScorerString = dllPerSizeScorer
  val defaultScorer = DllPerDsizeScorer

  implicit val operationScorerRead: scopt.Read[OperationScorer] = scopt.Read.reads {
    case `dllScorer` => DllScorer
    case `dllPerSizeScorer` => DllPerDsizeScorer
  }


//  operation completion types
  val complete = "complete"
  val minimal = "min"
  val maxEdgesPrefix = "maxEdges"
  val maxDepthPrefix = "maxDepth"
  val operationCompletionTypes = Array(complete, minimal, maxDepthPrefix+"-<k>", maxEdgesPrefix+"-<k>")
  val defaultOperationCompletionType = MaxDepth(3)
  val defaultOperationCompletionTypeString = maxDepthPrefix+"-3"

  val maxEdges = (maxEdgesPrefix+"-(\\d+)").r
  val maxDepth = (maxDepthPrefix+"-(\\d+)").r

  implicit val completionTypeRead: scopt.Read[OperationCompletionType] = scopt.Read.reads {
    case `complete` => Complete
    case `minimal` => Minimal
    case maxEdges(k) => MaxEdges(k.toInt)
    case maxDepth(k) => MaxDepth(k.toInt)
  }


  //  save frequencies
  val allPrefix = "all"
  val bestPrefix = "best"
  val frequencyTypes = Array(allPrefix+"-<k>",bestPrefix+"-<k>")
  val defaultFrequency = Best(1)
  val defaultFrequencyString = bestPrefix+"-"+1

  val all = (allPrefix+"-(\\d+)").r
  val best = (bestPrefix+"-(\\d+)").r
  implicit val frequencyTypeRead: scopt.Read[SaveFrequency] = scopt.Read.reads {
    case all(k) => All(k.toInt)
    case best(k) => Best(k.toInt)
  }

  // print help
  def printHelp(): Unit = {
    println("LearnPsdd 1.0")
    println("Usage: PSDD "+commands.mkString("|")+" [options]")
  }

  /**
    * Configuration
    *
    * An object of this class stores the configuration for all the possible functionalities of this code
    */
  case class Config(
                     out: String = null,
                     train: File = null,
                     valid: File = null,
                     test: File = null,
                     vtree: File = null,
                     psdd: File = null,
                     parameterCalculator: ParameterCalculator = defaultParameterCalculator,
                     vtreeMethod: String = defaultVtree,
                     debugLevel: Int = Int.MaxValue,
                     operationTypes: Seq[String] = defaultOperationTypes,
                     completionType: OperationCompletionType = defaultOperationCompletionType,
                     scorer: OperationScorer = defaultScorer,
                     maxIt: Int = Int.MaxValue,
                     structureChangeIt: Int = 1,
                     parameterLearningInt: Int = 3,
                     numComponentLearners: Int = 0,
                     keepSplitting: Boolean = false,
                     keepCloning: Boolean = false,
                     frequency: SaveFrequency = defaultFrequency,
                     parameterCalculators: Seq[ParameterCalculator] = null,
                     entropyOrder: Boolean=false
                     ){
    val configString: String = Array(
      "trainData:\t"+  (if(train == null) "null" else train.getPath),
      "validData:\t"+  (if(valid == null) "null" else valid.getPath),
      "testData:\t"+  (if(test == null) "null" else test.getPath),
      "vtreeFile:\t"+ (if(vtree == null) "null" else vtree.getPath),
      "psddFile:\t"+ (if(psdd== null) "null" else psdd.getPath),
      "out:\t"+ out
    ).mkString("\n")
  }

  /**
    * The learnEnsemblePsdd parser parses the options for learning an ensemble of psdds from data.
    */
  val learnEnsemblePsddParser = new scopt.OptionParser[Config](learnEnsemblePsdd+" softEM") {
    head("Learning the structure of an ensemble of psdds from data")

    opt[File]('p', "psdd").
      optional.
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}.
      text("If no psdd is provided, the learning starts from a mixture of marginals.\n")

    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }

    opt[String]('o', "out").
      required().
      valueName("<path>").
      action( (x, c) => c.copy(out = x) ).
      text("The folder for output\n")

    opt[Int]('c', "numComponentLearners").
      required().
      valueName("<numComponentLearners>").
      action((x, c) => c.copy(numComponentLearners = x)).
      text("The number of component learners to form the ensemble\n")

    opt[ParameterCalculator]('m',"smooth").
      optional().
      valueName("<smoothingType>").
      action((x,c) =>c.copy(parameterCalculator = x)).
      text (
        "default: " + defaultParameterCalculatorString + "\n" +
          parameterCalculators.map { case (k, v) => "\t * " + k + ": " + v }.mkString("\n")+"\n"
      )

    opt[OperationScorer]('s',"scorer").
      optional().
      valueName("<scorer>").
      action((x,c)=>c.copy(scorer = x)).
      text(
        "default: "+defaultScorerString+"\n" +
          scorers.map { case t => "\t * " + t }.mkString("\n")+"\n"
      )

    opt[Int]('e',"maxIt").
      optional().
      valueName("<maxIt>").
      action((x,c)=>c.copy(maxIt=x)).
      text(
        "this is the maximum number of ensemble learning iterations.\n"
      )


    opt[Int]("structureChangeIt").
      optional().
      valueName("<structureChangeIt>").
      action((x,c)=>c.copy(maxIt=x)).
      text(
        "this is the number of structure changes before a new round of parameter learning .\n"
      )

    opt[Int]("parameterLearningIt").
      optional().
      valueName("<parameterLearningIt>").
      action((x,c)=>c.copy(maxIt=x)).
      text(
        "this is the number of iterators for parameter learning before the structure of psdds changes again.\n"
      )


    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.out==null) failure("output path is required")
      if (c.numComponentLearners==0) failure("number of component learners to form the ensemble is required")
      success
    }

    help("help") text ("prints this usage text\n")

  }


  /**
    * The learnPSDD parser parses the options for learning a PSDD from data.
    * Furthermore, it provides information on the expected arguments
    */
  val learnPsddParser = new scopt.OptionParser[Config](learnPsdd +" "+ learnMethods.mkString("|")) {
    head("Learn the structure of a PSDD from data")

    opt[File]('p', "psdd").
      optional.
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}.
      text("If no psdd is provided, the learning starts from a mixture of marginals.\n")

    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }

    opt[String]('o', "out").
      required().
      valueName("<path>").
      action( (x, c) => c.copy(out = x) ).
      text("The folder for output\n")

    opt[ParameterCalculator]('m',"smooth").
      optional().
      valueName("<smoothingType>").
      action((x,c) =>c.copy(parameterCalculator = x)).
      text (
      "default: " + defaultParameterCalculatorString + "\n" +
        parameterCalculators.map { case (k, v) => "\t * " + k + ": " + v }.mkString("\n")+"\n"
    )

    opt[Seq[String]]('h', "opTypes").
      optional().
      valueName("<opType>,<opType>,...").
      action((x,c) => c.copy(operationTypes=x)).
      text(
        "default: " + defaultOperationTypes.mkString(",") + "\n" +
          "\toptions: "++ operationTypes.mkString(",")+"\n"+
          "\tIn split-k, k is the number of splits.\n"+
          "\tIn clone-k, k is the maximum number of parents to redirect to the clone.\n"
      )

    opt[OperationCompletionType]('c', "completion").
      optional().
      valueName("<completionType>").
      action((x,c)=>c.copy(completionType = x)).
      text(
        "default: "+defaultOperationCompletionTypeString+"\n" +
          operationCompletionTypes.map { case t => "\t * " + t }.mkString("\n")+"\n"
      )

    opt[OperationScorer]('s',"scorer").
      optional().
      valueName("<scorer>").
      action((x,c)=>c.copy(scorer = x)).
      text(
        "default: "+defaultScorerString+"\n" +
          scorers.map { case t => "\t * " + t }.mkString("\n")+"\n"
      )

    opt[Int]('e',"maxIt").
      optional().
      valueName("<maxIt>").
      action((x,c)=>c.copy(maxIt=x)).
      text(
        "For search, this is the maximum number of operations to be applied on the psdd.\n" +
          "\tFor bottom-up and top-down, at every level at most f*#vtreeNodesAtThisLevel operations will be applied.\n" +
          "\tdefault: maxInt\n"
      )

    opt[SaveFrequency]('f',"freq").
      optional().
      valueName("<freq>").
      action((x,c)=>c.copy(frequency = x)).
      text("method for saving psdds \n"+

        "\tdefault: "+defaultFrequencyString+"\n" +
        frequencyTypes.map { case t => "\t * " + t }.mkString("\n")+"\n"+
        "\tbest-k to only keep the best psdd on disk, all-k keeps all of the. A save attempt is made every k iterations\n"
      )


    opt[Int]('q',"debugLevel").
      optional().
      valueName("<level>").
      action((x,c)=>c.copy(debugLevel = x)).
      text("debug level\n")

    opt[Unit]("keepSplitting").
      optional().
      action((_,c)=>c.copy(keepSplitting = true)).
      text("Always for search (flag not needed then)\n")

    opt[Unit]("keepCloning").
      optional().
      action((_,c)=>c.copy(keepCloning = true)).
      text("Always for search (flag not needed then)\n")


    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.out==null) failure("output path is required")
      success
    }

    help("help") text ("prints this usage text\n")
  }

  /**
    * The SDD to PSDD parser parses the options for converting an SDD to a PSDD
    * and learning its parameters from data. (Choi et al, IJCAI 2015)
    * Furthermore, it provides information on the expected arguments
    */
  val sdd2psddParser = new scopt.OptionParser[Config](sdd2psdd) {
    override def showUsageOnError = true

    head("Learn a PSDD by doing parameter learning on an SDD")


    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }


    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }


    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('s', "sdd").
      required().
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}

    opt[String]('o', "out").
      required().
      valueName("<path>").
      action( (x, c) => c.copy(out = x) )

    opt[ParameterCalculator]('m',"smooth").
      optional().
      valueName("<smoothingType>").
      action((x,c) =>c.copy(parameterCalculator = x)).
      text (
      "default: " + defaultParameterCalculatorString + "\n" +
        parameterCalculators.map { case (k, v) => "\t * " + k + ": " + v }.mkString("\n"))


    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.psdd==null) failure("sdd is required")
      if (c.out==null) failure("output path is required")
      success
    }


    help("help") text ("prints this usage text\n")

  }

  /**
    * The Learn Parameters parser parses the options for learning the parameters for an existing psdd from data.
    * Furthermore, it provides information on the expected arguments
    */
  val learnParamsParser = new scopt.OptionParser[Config](parameterLearning) {
    override def showUsageOnError = true

    head("Learn the parameters of a PSDD")


    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }


    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('p', "psdd").
      required().
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}

    opt[String]('o', "out").
      required().
      valueName("<path>").
      action( (x, c) => c.copy(out = x) )

    opt[ParameterCalculator]('m',"smooth").
      optional().
      valueName("<smoothingType>").
      action((x,c) =>c.copy(parameterCalculator = x)).
      text (
      "default: " + defaultParameterCalculatorString + "\n" +
        parameterCalculators.map { case (k, v) => "\t * " + k + ": " + v }.mkString("\n"))


    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.psdd==null) failure("psdd is required")
      if (c.out==null) failure("output path is required")
      success
    }


    help("help") text ("prints this usage text\n")
  }


  /**
    * The Parameter Search parser parses the options for exploring different parameter learners for a given psdd  and data.
    * Furthermore, it provides information on the expected arguments
    */
  val paramSearchParser = new scopt.OptionParser[Config](paramSearch) {
    override def showUsageOnError = true

    head("Search for the best parameter calculator for a PSDD")


    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }


    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('p', "psdd").
      required().
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}

    opt[Seq[ParameterCalculator]]('m',"parameter calculators").
      optional().
      valueName("<paramCalc1>,<paramCalc2>,...").
      action((x,c) =>c.copy(parameterCalculators = x))


    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.psdd==null) failure("psdd is required")
      if (c.parameterCalculators==null) failure("parameterCalculators are required")
      success
    }

    help("help") text ("prints this usage text\n")

  }


  /**
    * The Vtree Learner parser parses the options for learning a vtree from data
    * Furthermore, it provides information on the expected arguments
    */
  val learnVtreeParser = new scopt.OptionParser[Config](learnVtree) {
    override def showUsageOnError = true

    head("learn/generate a vtree")


    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[String]('v', "vtreeMethod") optional() valueName (vtreeOptions.keys.mkString("|")) action { (x, c) => c.copy(vtreeMethod = x) } validate { x =>
      if (vtreeOptions.contains(x)) success else failure(x + " is not a shallowValid vtree method.")} text (
      "default: " + defaultVtree + "\n" +
        vtreeOptions.map { case (k, v) => "\t * " + k + ": " + v }.mkString("\n"))

    opt[String]('o', "out").
      required().
      valueName("<path>").
      action( (x, c) => c.copy(out = x) )

    opt[Unit]('e',"entropyOrder").
      optional().
      action((_,c)=>c.copy(entropyOrder = true)).
      text("choose prime variables to have lower entropy\n")

    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      success
    }


    help("help") text ("prints this usage text\n")
  }

  /**
    * The check parser parses the options for checking if a psdd is valild
    * Furthermore, it provides information on the expected arguments
    */
  val checkParser = new scopt.OptionParser[Config](check) {
    override def showUsageOnError = true

    head("Check if a PSDD is valid and calculate its likelihoods (in two ways)") //HERE

    opt[File]('d', "trainData").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(train = x) }

    opt[File]('b', "validData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(valid = x) }

    opt[File]('t', "testData").
      optional().
      valueName ("<file>").
      action { (x, c) => c.copy(test = x) }


    opt[File]('v', "vtree").
      required().
      valueName ("<file>").
      action { (x, c) => c.copy(vtree = x) }

    opt[File]('p', "psdd").
      required().
      valueName ("<file>").
      action{ (x,c) => c.copy(psdd = x)}

    checkConfig { c =>
      if (c.train==null) failure("train data is required")
      if (c.vtree==null) failure("vtree is required")
      if (c.psdd==null) failure("psdd is required")
      if (c.out==null) failure("output path is required")
      success
    }


    help("help") text ("prints this usage text\n")
  }

    def main(args: Array[String]): Unit = {

      if (args.length==0 || args(0).contains("-h")) printHelp()
      else args(0) match{

        // easy way to check if assertions are on or off
        case "assertTest" =>
          try {
            assert(false)
            println("assertions are off")
          } catch {
            case e: AssertionError => println("assertions are on")
            case e: Throwable => throw e
          }


//          learn an ensemble of psdds from data
        case `learnEnsemblePsdd` =>
          if (args.length<2){
            println("provide an ensemble learning method. Options: softEM.")
          }else{
            if (args(1) != "softEM"){
              println("currently the only ensemble learning method that is supported is: softEM.")
            }

            else learnEnsemblePsddParser.parse(args.drop(2), Config()) match {
              case None =>
              case Some(config) =>

                val out = new File(config.out)
                Output.init(out)
                Debug.init(new File(out, "debug"), config.debugLevel)

                Output.addWriter("cmd")
                Output.writeln(args.mkString(" "), "cmd")
                Output.writeln("learnEnsemblePsdd", "cmd")
                Output.writeln(config.configString, "cmd")
                Output.closeWriter("cmd")


                Debug.writeln("data")
                val trainData = Data.readFromFile(config.train)
                val validData = if (config.valid == null) trainData.empty else Data.readFromFile(config.valid)
                val testData = if (config.test == null) trainData.empty else Data.readFromFile(config.test)
                val data = new DataSets(trainData, validData, testData)

                Debug.writeln("psdd manager")
                val sddMgr = new SddManager(Vtree.read(config.vtree.getPath))
                val vtree = VtreeNode.read(config.vtree)
                val psddMgr = new PsddManager(sddMgr, true)

                Debug.writeln("make/read psdd")
                val psdds = for (i<- 0 until config.numComponentLearners) yield {
                  if (config.psdd != null) psddMgr.readPsdd(config.psdd, vtree, data, config.parameterCalculator).asInstanceOf[PsddDecision]
                  else psddMgr.newPsdd(vtree, data, config.parameterCalculator)
                }

                Debug.writeln("start learning...")

                val learner = new SoftEM(data, config.numComponentLearners, config.out, config.parameterCalculator, config.scorer, config.maxIt, config.structureChangeIt, config.parameterLearningInt)
                learner.learn(psdds, psddMgr)

                Output.writeln("==== DONE ====")
            }
          }


//          learn a psdd from data
        case `learnPsdd` =>
          if (args.length<2){
            println("provide a learnPsdd method. Options: "+learnMethods.mkString(", "))
          }
          else{
            val operationOrder = args(1)
            if (!learnMethods.contains(operationOrder)) {
              println("Error: incorrect learnPsdd method")
              printHelp()
            }

            else  learnPsddParser.parse(args.drop(2), Config()) match {
              case None =>
              case Some(config) =>

                val out = new File(config.out)
                Output.init(out)
                Debug.init(new File(out, "debug"), config.debugLevel)

                Output.addWriter("cmd")
                Output.writeln(args.mkString(" "), "cmd")
                Output.writeln("learnPsdd", "cmd")
                Output.writeln(config.configString, "cmd")
                Output.closeWriter("cmd")


                Debug.writeln("data")
                val trainData = Data.readFromFile(config.train)
                val validData = if (config.valid == null) trainData.empty else Data.readFromFile(config.valid)
                val testData = if (config.test == null) trainData.empty else Data.readFromFile(config.test)
                val data = new DataSets(trainData, validData, testData)

                Debug.writeln("psdd manager")
                val sddMgr = new SddManager(Vtree.read(config.vtree.getPath))
                val vtree = VtreeNode.read(config.vtree)
                val psddMgr = new PsddManager(sddMgr, true)

                Debug.writeln("make/read psdd")
                val psdd =
                  if (config.psdd!= null) psddMgr.readPsdd(config.psdd, vtree, data, config.parameterCalculator).asInstanceOf[PsddDecision]
                  else psddMgr.newPsdd(vtree, data, config.parameterCalculator)

                val operationFinders: Array[SpecificOperationQueue[_<:PsddOperation, _<:Object]] = config.operationTypes.map {
                  case cloneOp(k) => new CloneOperationQueue(new CloneOperationFinder(psddMgr, config.completionType, config.scorer, config.parameterCalculator, psdd, k.toInt))
                  case split(k) => new SplitOperationQueue(new SplitOperationFinder(psddMgr, config.completionType, config.scorer, config.parameterCalculator, psdd, k.toInt))
                }.toArray

                val learner = operationOrder match {
                  case `search` => new SearchLearner(operationFinders, config.parameterCalculator, config.maxIt, config.frequency)
                  case `bottomUp` => new BottomUpLearner(operationFinders, config.parameterCalculator, config.maxIt, config.keepSplitting, config.keepCloning, config.frequency)
                  case `topDown` => new TopDownLearner(operationFinders, config.parameterCalculator, config.maxIt, config.keepSplitting, config.keepCloning, config.frequency)
                }


                Debug.writeln("start learning...")
                learner.learn(psdd, psddMgr)

                Output.savePsdds(psdd, "final", asPsdd = true, asDot = true, asDot2 = true, asSdd = true, withVtree = true)
                Output.writeln("==== DONE ====")
            }
          }

//          Confert an sdd to a psdd and learn its parameters from data
        case `sdd2psdd` => sdd2psddParser.parse(args.drop(1), Config()) match {
          case None =>
          case Some(config) =>

            val pw = new PrintWriter(config.out+".cmd")
            pw.println(args.mkString(" "))
            pw.println("sdd2psdd")
            pw.println(config.configString)
            pw.flush()
            pw.close()

            val vtree = Vtree.read(config.vtree.getPath)
            val sddMgr = new SddManager(vtree)
            sddMgr.useAutoGcMin(false)

            val pVtree = VtreeNode.read(config.vtree)
            val psddMgr = new PsddManager(sddMgr, true)

            val trainData = Data.readFromFile(config.train)
            val validData = if (config.valid==null) trainData.empty else Data.readFromFile(config.valid)
            val testData = if (config.test==null) trainData.empty else Data.readFromFile(config.test)
            val data = new DataSets(trainData, validData, testData)

            val psdd = psddMgr.readPsddFromSdd(config.psdd, pVtree, data, config.parameterCalculator)

            PsddQueries.save(psdd, new File(config.out))

            println(dataSets.map(_+"Ll").mkString("\t"))
            println(dataSets.map(PsddQueries.logLikelihood(psdd,_)).mkString("\t"))
        }

//          learn the parameters of an existing psdd from data
        case `parameterLearning` => learnParamsParser.parse(args.drop(1), Config()) match {
          case None =>
          case Some(config) =>

            val pw = new PrintWriter(config.out+".cmd")
            pw.println(args.mkString(" "))
            pw.println("parameterLearning")
            pw.println(config.configString)
            pw.flush()
            pw.close()

            val sddMgr = new SddManager(Vtree.read(config.vtree.getPath))
            sddMgr.useAutoGcMin(false)

            val vtree = VtreeNode.read(config.vtree)
            val psddMgr = new PsddManager(sddMgr, true)

            val trainData = Data.readFromFile(config.train)
            val validData = if (config.valid==null) trainData.empty else Data.readFromFile(config.valid)
            val testData = if (config.test==null) trainData.empty else Data.readFromFile(config.test)
            val data = new DataSets(trainData, validData, testData)

            val psdd = psddMgr.readPsdd(config.psdd, vtree, data, config.parameterCalculator)
            PsddQueries.save(psdd, new File(config.out + ".psdd"))

            println(dataSets.map(_+"Ll").mkString("\t"))
            println(dataSets.map(PsddQueries.logLikelihood(psdd,_)).mkString("\t"))
        }

//          learn a vtree from data
        case `learnVtree` => learnVtreeParser.parse(args.drop(1), Config()) match {
          case None =>
          case Some(config) =>
            val pw = new PrintWriter(config.out+".cmd")
            pw.println(args.mkString(" "))
            pw.println("learnVtree")
            pw.println(config.configString)
            pw.flush()
            pw.close()

            val train = Data.readFromFile(config.train)
            val vtreeNode = config.vtreeMethod match {
              case "balancedBU" => VtreeNode.balancedBottomUp(train.vars.sorted)
              case "balanced-ord" => VtreeNode.balanced(train.vars.sorted)
              case "rightLinear-ord" => VtreeNode.rightLinear(train.vars.sorted)
              case "leftLinear-ord" => VtreeNode.leftLinear(train.vars.sorted)
              case "balanced-rand" => VtreeNode.balanced(Random.shuffle(train.vars.toSeq).toArray)
              case "rightLinear-rand" => VtreeNode.rightLinear(Random.shuffle(train.vars.toSeq).toArray)
              case "leftLinear-rand" => VtreeNode.leftLinear(Random.shuffle(train.vars.toSeq).toArray)
              case "pairwiseWeights" => VtreeLearner.learnExhaustiveTopDown(train)
              case "miGreedyBU" => VtreeLearner.learnGreedyBottomUp(train)
              case "miMetis" => VtreeLearner.learnMetisTopDown(train, 1, config.out, config.entropyOrder)
              case "miBlossom" => VtreeLearner.learnBlossomBottomUp(train, config.out, config.entropyOrder)
            }
            vtreeNode match{
              case vtree: VtreeInternal => vtree.save(new File(config.out + ".vtree"))
              case _ => println("There were no variables, so no vtree could be learned")
            }
        }

//          test different parameter learners for a given psdd
        case `paramSearch` => paramSearchParser.parse(args.drop(1), Config()) match {
          case None =>
          case Some(config) =>

            val pw = new PrintWriter(config.out+".cmd")
            pw.println(args.mkString(" "))
            pw.println("parameterLearning")
            pw.println(config.configString)
            pw.flush()
            pw.close()

            val sddMgr = new SddManager(Vtree.read(config.vtree.getPath))
            sddMgr.useAutoGcMin(false)

            val vtree = VtreeNode.read(config.vtree)
            val psddMgr = new PsddManager(sddMgr, true)

            val trainData = Data.readFromFile(config.train)
            val validData = if (config.valid==null) trainData.empty else Data.readFromFile(config.valid)
            val testData = if (config.test==null) trainData.empty else Data.readFromFile(config.test)
            val data = new DataSets(trainData, validData, testData)

            println("params,trainLl,validLl,testLl")
            val psdd = psddMgr.readPsdd(config.psdd, vtree, data)
            val results = config.parameterCalculators.map { calc =>
              psddMgr.calculateParameters(psdd, calc, psdd)
              val res = (calc, PsddQueries.logLikelihood(psdd, "train") / data.train.total, PsddQueries.logLikelihood(psdd, "valid") / data.valid.total, PsddQueries.logLikelihood(psdd, "test") / data.test.total)
              println(res._1+","+res._2+","+res._3+","+res._4)
              res
            }

            println()
            val best = results.maxBy(_._3)
            println(best._1+","+best._2+","+best._3+","+best._4)
        }

//          check if the given psdd is valid (for debug purpose)
        case `check` =>checkParser.parse(args.drop(1), Config()) match {
          case None =>
          case Some(config) =>

            val sddMgr = new SddManager(Vtree.read(config.vtree.getPath))
            sddMgr.useAutoGcMin(false)


            print("Prepare psdd manager...")
            val vtree = VtreeNode.read(config.vtree)
            val psddMgr = new PsddManager(sddMgr, true)
            println(" done!")

            print("Read data...")
            val trainData = Data.readFromFile(config.train)
            val validData = if (config.valid == null) trainData.empty else Data.readFromFile(config.valid)
            val testData = if (config.test == null) trainData.empty else Data.readFromFile(config.test)
            val data = new DataSets(trainData, validData, testData)
            println(" done!")

            print("Read Psdd from "+config.psdd+"...")
            val psdd = psddMgr.readPsdd(config.psdd, vtree, data)
            println(" done!")

            println()

            print("Calculate train ll (fast)...")
            println("\t" + PsddQueries.logLikelihood(psdd, "train") / data.train.total)
            print("Calculate valid ll (fast)...")
            println("\t" + PsddQueries.logLikelihood(psdd, "valid") / data.valid.total)
            print("Calculate test ll (fast)... ")
            println("\t" + PsddQueries.logLikelihood(psdd, "test") / data.test.total)

            println()


            print("Calculate train ll (slow)...")
            println("\t" + data.train.weightedIterator.map { case (example, weight) => PsddQueries.logProb(psdd, example) * weight }.sum / data.train.total)
            print("Calculate valid ll (slow)...")
            println("\t" + data.valid.weightedIterator.map { case (example, weight) => PsddQueries.logProb(psdd, example) * weight }.sum / data.valid.total)
            print("Calculate test ll (slow)...")
            println("\t" + data.test.weightedIterator.map { case (example, weight) => PsddQueries.logProb(psdd, example) * weight }.sum / data.test.total)

            println()

            print("Check psdd validity...")
            val valid = PsddQueries.isValid(psdd)
            println(if (valid) "Valid!" else "Invalid!")

        }

          // This is some scratch space that can be used to test stuff during implementation.
        case `scratch` =>
            println("scratch")
            // write scratch code here

      }

    }
}
