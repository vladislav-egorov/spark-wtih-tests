import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class WordCounterTest extends FunSuite with DataFrameSuiteBase {
  test("Word count test") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    assert(input1.count() == 3)
  }
}