import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object LinkGraph {
    def main(args: Array[String]) {    
        val sparkConf = new SparkConf().setAppName("LinkGraph")
        val sc = new SparkContext(sparkConf)

        val input = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/WikiArticle/articles")
// your output directory from the last assignment
        val page = input.map{ l =>
            val pair = l.stripPrefix("(").stripSuffix(")").split("\t", 2);
            (pair(0), pair(1)) 
        }
// get the two fields: title and text
        val links = page.map(r => (r._1, extractLinks(r._2)))
// extract links from text
        val linkcounts = links.map(r => (r._1,r._2.split("\t").length))
// count number of links
// save the links and the counts in compressed format (save your disk space) 
        links.saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/LinkGraph/links", classOf[GzipCodec])
        linkcounts.saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/LinkGraph/links-counts", classOf[GzipCodec])
    }

    def extractLinks(text: String) : String = {
// you will need to work on a way to extract the links
        val outlink_pattern1 = """\[\[[^:]+?\]\]""".r //match beside [[xxxx:xxxxx]]
        
        val outlink_pattern2 = """\[\[[^|#,\]]+""".r//find [[23123123

        val link_content = outlink_pattern1.findAllIn(text).toArray.map(l=>l.replace("]]",""))//as [[xxxxx or [[xxxx#xxxx
        val clean_content = link_content.flatMap(l=> outlink_pattern2 findAllIn l)
        clean_content.map(l=>l+"]]").mkString("\t") 
    }

}
