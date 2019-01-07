package operations

abstract class OperationCompletionType

case object Complete extends OperationCompletionType
case object Minimal extends OperationCompletionType
case class MaxDepth(k: Int) extends OperationCompletionType
case class MaxEdges(k: Int) extends OperationCompletionType
