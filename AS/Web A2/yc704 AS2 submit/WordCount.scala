import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex



object WordCount {
    def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)
    val one = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/sbt/src/main/scala/one.txt")
    val two = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/sbt/src/main/scala/two.txt")
    val combine = one.union(two)//combine two files 
    val pat = """[^A-Za-z0-9 ]""".r



    val wordsinone = one.flatMap(line => pat.replaceAllIn(line," ").split("\\s+"))
    println("\n\n\nThe total number of words in one.txt is " + wordsinone.count + "\n\n\n\n\n\n")
    val uniquewordsone = one.flatMap(line => pat.replaceAllIn(line," ").toLowerCase().split("\\s+")).map(word=>(word,1)).reduceByKey(_+_)
    println("\n\n\nThe total number of unique words in one.txt is "+uniquewordsone.count + "\n\n\n\n\n\n")
    
    val counts = combine.flatMap(line=> pat.replaceAllIn(line," ").toLowerCase().split("\\s+")).map(word=>(word,1)).reduceByKey(_+_)
    counts.saveAsTextFile("wcOutput")
    sc.stop()
    }
}
