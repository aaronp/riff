package riff.raft.append
import org.scalatest.FunSuite
import riff.RiffSpec

class HasInfoForAppendTest extends RiffSpec {

  "HasInfoForAppend.onAppend" should {
    "return a failed reply if given an earlier term" in {

    }
    "return a successful reply for any term > our own term" in {

    }
    "return a zero match index if the previous log term doesn' match" in {

    }
  }
}
