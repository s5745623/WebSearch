import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._


object WikiArticle {
    def main(args: Array[String]) {
    
        val sparkConf = new SparkConf().setAppName("Wiki Article")
        val sc = new SparkContext(sparkConf)

        val txt = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/WikiArticle/src/main/scala/output_small.txt") 
        val articlesnum = sc.accumulator(0)
        val xml = txt.map{ l =>
            val line = XML.loadString(l)
            val title = (line \ "title").text
            val text = (line \\ "text").text 
            (title,text) 
        }


        val articles = xml.filter( r => isArticle(r._2.toLowerCase,articlesnum)).map(r => r._1+"\t"+r._2).cache()
        articles.saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/WikiArticle/articles")
        println("\n\n\n\n\nThe total number of articles is "+articlesnum.value.toString)
        
        
        
//wordcount        
        val pattern1 = """[^A-Za-z0-9 ]""".r
        //val wordsinarticles = articles.flatMap(line=>line.toString.toLowerCase.split("\\W+")).filter(word=>!word.isEmpty)
        val wordsinarticles = articles.flatMap(line => pattern1.replaceAllIn(line," ").toLowerCase.split("\\W+"))
        println("\n\n\n\nThe total number of words in articles is " + wordsinarticles.count)


//unique wordcount bonus
        val uniquewords = articles.flatMap(line => pattern1.replaceAllIn(line, " ").toLowerCase.split("\\s+")).map(word => (word,1)).reduceByKey(_+_)
        uniquewords.saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/WikiArticle/wordcount")
        println("\n\n\n\nThe total number of unique words in articles is " + uniquewords.count)
        
    }

        def isArticle(line: String, articlesnum:org.apache.spark.Accumulator[Int]):Boolean = {
        val rediect_pattern = """#(REDIRECT|redirect)""".r
        val stubs_pattern = "-stub}}".r
        val disambiguation_pattern = """\{\{disambig(uation)?\}\}""".r
    
        if (((rediect_pattern findAllIn line).toArray.length == 0) && ((stubs_pattern findAllIn line).toArray.length == 0) && ((disambiguation_pattern findAllIn line).toArray.length == 0)){
            articlesnum+=1
            return true
        
        }
    
        return false
    }
}
