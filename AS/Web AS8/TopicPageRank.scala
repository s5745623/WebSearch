import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.compress.GzipCodec 
import org.apache.spark.broadcast.Broadcast

object TopicPageRank {
      def main(args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("TopicPageRank").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)
    
      val linkgraph = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/LinkGraph/links/part-*")
      
      val links = linkgraph.map{ l =>
        val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2);
        val title = "[["+pair(0)+"]]"; 
        val outlink= pair(1).split("\t");
        (title, outlink)
      } 
      
      var ranks = linkgraph.map{ l =>
            val pair = l.stripPrefix("(").stripSuffix(")").split(",", 2);
            val title = "[[" + pair(0) + "]]"
            var rank = 1.0
            (title,rank)
      }
      

      val football = sc.textFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/TopicPageRank/src/main/scala/part00000_1").map(r => "[[" + r + "]]").collect()
      val bTopicPages = sc.broadcast(football.toSet)
      val topicPages = bTopicPages.value

      val ITERATION = 10
      for (i <- 0 to ITERATION) {
            val contribs = links.join(ranks).flatMap {
                  case (title, (links, rank)) =>
                  links.map(dest => (dest, rank / links.size))
            }

            var onTopicRank = contribs.reduceByKey( _+_ ).filter( x => topicPages.contains(x._1)).mapValues(0.15 + 0.85 * _ )
            var offTopicRank = contribs.reduceByKey( _+_ ).filter( x => ! topicPages.contains(x._1)).mapValues(0.85 * _ )
            ranks = onTopicRank.union(offTopicRank) 
      }
      ranks.sortBy(_._2, false).map(r => r._1 + "\t" + r._2).saveAsTextFile("/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/TopicPageRank/TopicPageRank", classOf[GzipCodec])
      } 
}