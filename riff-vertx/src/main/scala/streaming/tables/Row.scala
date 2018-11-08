package streaming.tables
import cats.Show
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import monix.reactive.subjects.Var

import scala.reflect.ClassTag
trait Column {
  type T
  def pos: Int
  def data: T
}

class Row(cells: Array[Column])

object Row {

  object example {
    case class Person(id: Int, name: String)
    val people: Observable[Person] = Observable.fromIterable((1 to 10).map { i =>
      Person(i, "guy " + i)
    })
    def people(id: Int, from: Int, size: Int): Observable[Person] = {
      Observable.fromIterable((from until (from + size)).map(i => Person(id, s"person $i")))
    }
    def friendsOf(id: Int, from: Int, size: Int): Observable[Person] = {
      Observable.fromIterable((from until (from + size)).map(i => Person(i + 100, s"friend $i of $id")))
    }
    def addresses(id: Int, from: Int, size: Int) = {
      Observable.fromIterable((from until (from + size)).map(i => s"address $i of $id"))
    }

    val dudes = Var[Person](Person(0, "dave"))

  }

  object viewServer {

    case class ViewPort(row: Int, size: Int)

    case class TableState(cols: Seq[String], view: ViewPort, lookup: Lookup) {

      def update(newCols: Seq[String], newPort: ViewPort): TableState = {
        val removedCols: Seq[String] = cols diff (newCols)
        val addedCols: Seq[String]   = newCols diff (cols)

        this
      }
    }

    /**
      * We need to produce results w/ an index as the result of querying a range
      *
      * @tparam T
      */
    trait IndexedValue[T] {
      def indexOf(value: T): Int
    }
    object IndexedValue {
      case class Inst[T](index: Int, value: T)
      implicit object InstImpl extends IndexedValue[Inst[_]] {
        override def indexOf(value: Inst[_]): Int = value.index
      }
    }

    trait Cell {
      type T
      def value: T
    }

    case class ColumnSource(column: String, values: Observable[Cell])
    case class TableSource(values: Observable[ColumnSource])

    object TableRender {
      case class Update[T](column: String, index: Int, value: T)

      case class Renderer[T: Show](updates: Observable[Update[T]])
    }

    trait Lookup {
      def find[T: ClassTag](colName: String): DataSource
    }

    trait DataSource {
      type Criteria
      type T
      def query(criteria: Criteria, offset: ViewPort): Observable[(Int, T)]
    }

//    def run(initialState: TableState, colInputs: Observable[Seq[String]], view: Observable[ViewPort]): Observable[TableUpdate] = {
//
//      val o: Observable[(Seq[String], ViewPort)] = colInputs.combineLatest(view)
//
//      val stateUpdate = o.foldLeftF(initialState) {
//        case (state, (cols, port)) => state.update(cols, port)
//      }
//      ???
//    }

  }

}
