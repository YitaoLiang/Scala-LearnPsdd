package util

import sdd.{Sdd, SddManager, Vtree, VtreeType}

import scala.io.Source

/**
  * Created by jessa on 3/30/17.
  */
object MutexSddGeneration {

  def main(args: Array[String]): Unit = {

    for (data <- Array("test","adult","covtype")) {
      println(data)
      val constraints_path = "/home/jessa/Code/LearnPsdd/PSDD2/multivalData/binarizedData/" + data + ".multiAtts"
      val sdd_path = "/home/jessa/Code/LearnPsdd/PSDD2/multivalData/" + data + ".sdd"
      val sdd_dot_path = "/home/jessa/Code/LearnPsdd/PSDD2/multivalData/" + data + ".dot"
      val vtree_path = "/home/jessa/Code/LearnPsdd/PSDD2/multivalData/" + data + ".vtree"

      val multiVars = Source.fromFile(constraints_path).getLines().map { line => line.split("-").map(_.toInt + 1) }.toArray.filterNot(_.isEmpty).map(ar =>(ar.head, ar(1)))

      val nbVars = multiVars.map(_._2).max
      println(nbVars)

      val mgr = new SddManager(new Vtree(nbVars, VtreeType.Balanced))

      mgr.useAutoGcMin(false)
//      val sdd = new Sdd(true, mgr)

      val sdd = multiVars.map { case (mi, ma) =>
        val vars = (mi to ma).toSet
        if (vars.size>1) {
          vars.map { i =>
            new Sdd(i, mgr).conjoin((vars - i).map(j => new Sdd(-j, mgr)).reduce(_.conjoin(_)))
          }.reduce(_.disjoin(_))
        } else new Sdd(true, mgr)
      }.reduce(_.conjoin(_))
      sdd.ref()
      mgr.minimize()
      sdd.deref()
      sdd.save(sdd_path)
      sdd.saveAsDot(sdd_dot_path)
      println(sdd.getVisualization)
      println(sdd.getVtree.getVisualization)
      sdd.getVtree.save(vtree_path)
      mgr.free()
    }

  }
}
