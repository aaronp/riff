package riff.raft.integration.simulator
import scala.Ordered._

/**
* Used to combine lists which are already in some order.
  *
  * You'd used this this instead of just {{{(left ++ right).sorted}}} if you wanted to maintain the existing order as a
  * tie-breaker for elements which would be equated as equal by the Ordering
  *
  * e.g. {{{
  * MergeSorted(List((1,a), (1,a), (3,a)), List((1,b), (2,b), (3,b))) yields
  * List((1,a), (1, a), (1,b), (2,b), (3, a), (3,b))
  * }}}
  */
object MergeSorted {

  def apply[A: Ordering](left: List[A], right: List[A], result: List[A] = Nil): List[A] = {
    right match {
      case Nil => result ::: left
      case head :: rightTail =>
        val (leftHead, leftTail) = left.span(_ <= head)
        val sorted               = leftHead :+ head
        apply(leftTail, rightTail, result ::: sorted)
    }
  }
}
