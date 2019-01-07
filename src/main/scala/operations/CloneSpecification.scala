package operations

import structure.{PsddDecision, PsddElement}

import scala.collection.mutable

case class CloneSpecification(cloneAll: Boolean, clones: Array[Boolean], parents: Array[mutable.Set[(PsddDecision, PsddElement)]]) {
  override def toString: String = (cloneAll, if (clones==null) null else clones.mkString("[",",","]"), if (parents==null) null else parents.mkString("[",",","]")).toString()
}
