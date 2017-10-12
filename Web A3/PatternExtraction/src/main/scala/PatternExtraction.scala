import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex

object PatternExtraction {
    def main(args: Array[String]){

    val conf = new SparkConf().setAppName("Pattern Extraction").setMaster("local")
    val sc = new SparkContext(conf)
    val file1 = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/PatternExtraction/src/main/scala/one.txt")
    val file2 = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/PatternExtraction/src/main/scala/two.txt")
    val file = file1.union(file2)

    //pattern
    val quot_pattern = "(\"(.*?)\")".r //""
    val year_pattern = "(\\d{4})".r //2014
    val date_pattern = """(Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)([a-z]+)? ([\d]{1,2}[^0-9])( [\d]{4})?""".r //June 30, 2014
    val quest_pattern = "(Jan|Feb|Mar|Apr|May|June|July|Aug|Sept|Oct|Nov|Dec)([a-z]+)? [0-9]{2}\\?[0-9]{2}".r//June 16?19

    
    val quotation = file.flatMap(line => quot_pattern.findAllIn(line)).count
    println("The total number of occurrences of quotations in both files : "+quotation)
    
    val year = file.flatMap(line => year_pattern.findAllIn(line)).count
    println("The total number of occurrences of years in both files : "+year)

    val question = file.flatMap(line => quest_pattern.findAllIn(line)).count
    val date = file.flatMap(line => date_pattern.findAllIn(line)).count
    println("The total number of occurrences of dates in both files : "+(date+question))

    }
}