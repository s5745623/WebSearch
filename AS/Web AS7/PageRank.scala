
import scala.util.matching.Regex
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object PageRank {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val input = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/LinkGraph/links/part-00000")// your output directory from the last assignment
        
        val links = input.map{ l=>
            val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2);
            val title = "[["+pair(0)+"]]"
            val outlink= pair(1).split("\t").toList
            (title, outlink)
        }// Load RDD of (page title, links) pairs 



        var ranks = input.map{ l=>
            val pair = l.stripPrefix("(").stripSuffix(")").split(",",2);
            val title = "[["+pair(0)+"]]"
            var rank = 1.0
            (title, rank)
        }// Load RDD of (page title, rank) pairs


        val ITERATION = 20
        // Implement PageRank algorithm
        for (i <- 0 to ITERATION) {
            val contribs = links.join(ranks).flatMap { 
                case (title, (links, rank)) =>
                    links.map(dest => (dest, rank / links.size))
                }
            ranks = contribs.reduceByKey( _+_ ).mapValues(0.15 + 0.85 * _ )

        }
        ranks.cache()


        // Sort pages by their PageRank scores descending
        ranks.sortBy(_._2,false).map(r => r._1 + "\t" + r._2).saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/PageRank/pageranks", classOf[GzipCodec])
        
        //person page rank
        val articles = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/WikiArticle/articles/part-00000")
        val page = articles.map{ 
            l =>val pair = l.stripPrefix("(").stripSuffix(")").split("\t", 2)
                (pair(0), pair(1))
            }
        val persons = page.map{
             r => isPerson(r._1,r._2)
            }.filter(line => line !="None").map(a => "[["+ a +"]]" )
        val set = persons.collect.toSet
        // filter out the ranks with the title of person
        val personranks = ranks.filter(line => set.contains(line._1))
         
        personranks.sortBy(_._2,false).map(r => r._1 + "\t" + r._2).saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/PageRank/personrank")

    } 

    def isPerson(title: String, context:String) : String ={
            val pat = """\[\[Category:\d+\s+(B|b)irths\]\]""".r
            var pattern = (pat findAllIn context).toList
            if (pattern.isEmpty == false){
              title
            }

            else{
              "None"
            }
   
    }
}