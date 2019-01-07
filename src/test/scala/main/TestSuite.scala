package main

import algo._
import operations._
import structure._
import util._
import org.junit.runner.RunWith
import org.junit.runners.Suite

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[ParameterCalculatorTest],
  classOf[PsddManagerTest],
  classOf[VtreeNodeTest]
))
class TestSuite