import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions

object WordCount extends App {
  //val test = <a>{"test"}</a>
  //println(test)
  //System.exit(0)
  val sc = SparkContext.getOrCreate(new SparkConf().setAppName("wordcount").setMaster("local[*]"))
  sc.setLogLevel("ERROR")
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  groupTest
  def wordCount() = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.StringType
    import org.apache.spark.sql.types.StructField
    import org.apache.spark.sql.types.StructType
    val xref = sc.textFile("file:\\H:\\test\\sample.txt").map(line => line.split(","))
    val xrefrowrdd = xref.map { x => Row.fromSeq(x) }
    val xreffields = List("id", "name", "designation")
    val xrefschema = StructType(xreffields.map(fieldName => StructField(fieldName, StringType, true)))
    val xrefdf = sqlContext.createDataFrame(xrefrowrdd, xrefschema)
    xrefdf.registerTempTable("XREF")
    //xrefdf.filter($"id".contains(1)).show
    //xrefdf.show()
    //sqlContext.sql("select upper(name) from XREF where id=1").show
    xrefdf.printSchema
  }
  def groupTest()={
    val getFirst = new getFirst
    sqlContext.udf.register("getFirst", new getFirst)
    val myDF = sc.parallelize(Seq(("1","1","0.1","",""),("1","2","0.8","","siva"),("1","3","","0.3","")), 2).toDF("patient_id","visit_id","var1","var2","var3")
    myDF.show
    myDF.sort($"patient_id", $"visit_id".desc).groupBy($"patient_id").agg(getFirst($"var1"),getFirst($"var2"),getFirst($"var3")).show
    //.agg(first("var1",true),first("var2",true),first("var3",true)).show
    
  }
}