package structure

import junit.framework.TestCase

class VtreeNodeTest extends TestCase{

  def testRightLinear() {
    val rl = VtreeNode.rightLinear(Array(1, 2, 3, 4))
    val correctRl = new VtreeInternal(1, null, null, null)
    correctRl.left = new VtreeInternalVar(0, correctRl, 1)
    val internal3 = new VtreeInternal(3, correctRl, null, null)
    internal3.left = new VtreeInternalVar(2, internal3, 2)
    correctRl.right = internal3
    val internal5 = new VtreeInternal(5, internal3, null, null)
    internal5.left = new VtreeInternalVar(4, internal5, 3)
    internal5.right = new VtreeInternalVar(6, internal5, 4)
    internal3.right = internal5

    assert(rl.visualization == correctRl.visualization, "incorrect RL tree: "+rl.visualization)
  }

  def testLeftLinear() {
    val ll = VtreeNode.leftLinear(Array(1, 2, 3, 4))
    val correctLl = new VtreeInternal(5, null, null, null)
    correctLl.right = new VtreeInternalVar(6, correctLl, 4)
    val internal3 = new VtreeInternal(3, correctLl, null, null)
    internal3.right = new VtreeInternalVar(4, internal3, 3)
    correctLl.left = internal3
    val internal5 = new VtreeInternal(1, internal3, null, null)
    internal5.right = new VtreeInternalVar(2, internal5, 2)
    internal5.left = new VtreeInternalVar(0, internal5, 1)
    internal3.left = internal5

    assert(ll.visualization == correctLl.visualization, "incorrect LL tree: "+ll.visualization)
  }

  def testBalanced() {
    val balanced = VtreeNode.balanced(Array(1, 2, 3, 4, 5))
    val correctBalanced = new VtreeInternal(3, null, null, null)
    val internal2 = new VtreeInternal(1, correctBalanced, null, null)
    internal2.left = new VtreeInternalVar(0, internal2, 1)
    internal2.right = new VtreeInternalVar(2, internal2, 2)
    val internal5 = new VtreeInternal(5, correctBalanced, null, null)
    internal5.left = new VtreeInternalVar(4, internal5, 3)
    val internal7 = new VtreeInternal(7, internal5, null, null)
    internal7.left = new VtreeInternalVar(6,internal7, 4)
    internal7.right = new VtreeInternalVar(8,internal7, 5)
    internal5.right = internal7
    correctBalanced.left = internal2
    correctBalanced.right = internal5

    assert(balanced.visualization == correctBalanced.visualization, "incorrect balanced tree: "+balanced.visualization)
  }

}
