import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf

object Samplehbase {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SampleHBaseRead").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    def catalog = s"""{
       |"table":{"namespace":"default", "name":"tableSample"},
       |"rowkey":"key",
       |"columns":{
         |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
         |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
         |"col2":{"cf":"cf1", "col":"col2", "type":"string"},
         |"col3":{"cf":"cf2", "col":"col1", "type":"string"}
       |}
     |}""".stripMargin

    val df = sqlContext.read.options(Map("catalog"->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()
    
    df.show()
    
    sc.stop()
  }
}
