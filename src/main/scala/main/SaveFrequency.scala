package main

abstract class SaveFrequency

case class All(k: Int) extends SaveFrequency
case class Best(k: Int) extends SaveFrequency


