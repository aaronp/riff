package riff

package object raft {

  type Term     = Int
  type LogIndex = Int

  def isMajority(numberReceived: Int, clusterSize: Int) = {
    numberReceived > clusterSize / 2
  }

}
