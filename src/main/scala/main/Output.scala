package main

import java.io.{PrintWriter, File}

import operations.PsddQueries
import structure.{VtreeInternal, PsddNode}

import scala.collection.mutable


object Output {
  private var outFolder: File = null


  var modelFolder: File = null
  private val counters = mutable.Map.empty[String,Int]

  private val writers = mutable.Map.empty[String, PrintWriter]

  def savePsdds(psdd: PsddNode, name: String, asPsdd: Boolean=true, asDot: Boolean=false, asDot2:Boolean=false, asSdd:Boolean=false, withVtree:Boolean=false): Unit ={
    if (asPsdd) PsddQueries.save(psdd, new File(modelFolder, name+".psdd"))
    if (asPsdd) PsddQueries.saveAsDot(psdd, new File(modelFolder, name+".dot"))
    if (asPsdd) PsddQueries.saveAsDot2(psdd, new File(modelFolder, name+".dot2"))
    if (asPsdd) PsddQueries.saveAsSdd(psdd, new File(modelFolder, name+".sdd"))
    if (withVtree) psdd.vtree match{
      case vtree: VtreeInternal => vtree.save(new File(modelFolder, name+".vtree"))
      case _ =>
    }
  }

  def addCounter(counter: String) = counters(counter) = 0

  def getCounter(counter: String) = counters.get(counter) match {
    case None => -1
    case Some(count) => count
  }
  def increment(counter: String) = counters(counter) +=1
  def reset(counter: String) = counters(counter) =0

  def addWriter(writer: String, name: String = null) = writers(writer) = new PrintWriter(new File(outFolder, if (name!=null) name else writer))
  def write(text: String, writer: String="out") = {
    print(text)
    writers(writer).write(text)
    writers(writer).flush()
  }
  def writeln(text: String, writer: String="out") = write(text+"\n", writer)
  def closeWriter(writer: String) = {writers(writer).close(); writers.remove(writer)}

  def init(outFolder: File): Unit = {
    this.outFolder = outFolder
    this.modelFolder = new File(outFolder, "models/")
    modelFolder.mkdirs()
    modelFolder.mkdir()

    counters.clear()
    writers.clear()
    addWriter("out")
  }
}

object Debug {

  val nano:Double = math.pow(10,-9)

  private var debugFolder: File = null
  private var modelFolder: File = null

  private var debugLevel: Int = 0

  def level = debugLevel

  private val counters = mutable.Map.empty[String,Int]

  private val writers = mutable.Map.empty[String, PrintWriter]

  def savePsdds(psdd: PsddNode, name: String, asPsdd: Boolean=true, asDot: Boolean=false, asDot2:Boolean=false, asSdd:Boolean=false, withVtree:Boolean=false): Unit ={
    if (asPsdd) PsddQueries.save(psdd, new File(modelFolder, name+".psdd"))
    if (asPsdd) PsddQueries.saveAsDot(psdd, new File(modelFolder, name+".dot"))
    if (asPsdd) PsddQueries.saveAsDot2(psdd, new File(modelFolder, name+".dot2"))
    if (asPsdd) PsddQueries.saveAsSdd(psdd, new File(modelFolder, name+".sdd"))
    if (withVtree) psdd.vtree match{
      case vtree: VtreeInternal => vtree.save(new File(modelFolder, name+".vtree"))
      case _ =>
    }
  }

  def addCounter(counter: String) = counters(counter) = 0
  def getCounter(counter: String) = counters(counter)
  def increment(counter: String) = counters(counter) +=1

  def addWriter(writer: String, name: String = null, debugLevel:Int = 0) = if (debugLevel<=this.debugLevel) writers(writer) = new PrintWriter(new File(debugFolder, if (name!=null) name else writer))
  def write(writer: String, debugLevel: Int, text: => String) = if (debugLevel<=this.debugLevel) {writers(writer).write(System.nanoTime()*nano+";\t"+text); writers(writer).flush()}
  def writeln(writer: String, debugLevel: Int, text: =>String) = write(writer, debugLevel, text+"\n")
  def writeln(text: => String): Unit = writeln("debug",1,text)
  def closeWriter(writer: String) = {writers(writer).close(); writers.remove(writer)}

  def init(debugFolder: File, debugLevel: Int): Unit = {
    this.debugFolder = debugFolder
    debugFolder.mkdirs()
    debugFolder.mkdir()
    this.modelFolder = new File(debugFolder, "models/")
    modelFolder.mkdir()

    this.debugLevel = debugLevel

    counters.clear()
    writers.clear()
    addWriter("debug")
  }
}
