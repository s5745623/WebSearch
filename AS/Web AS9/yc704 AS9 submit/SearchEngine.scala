import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec



object SearchEngine {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("SearchEngine").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        //query 
        val query_1 =  "colleague football"
        val query_2 = "Georgetown alumni"
        val query_3 = "Hillary Clinton"
        //weight
        val weight_1 ="1.0,1.8"
        val weight_2 = "1.8,1.2"
        val weight_3 = "1.2,2.0"

        val articles = sc.textFile("../WikiArticle/articles/part-*") 
        val pageranks = sc.textFile("../PageRank/pageranks/part-*")
        // val articles = sc.textFile("article_test.txt") 
        // val pageranks = sc.textFile("pagerank_test.txt")

        val page = articles.map{ 
            l =>val pair = l.stripPrefix("(").stripSuffix(")").trim().split("\t", 2)
                ("[["+pair(0)+"]]", pair(1))
            }
        //get cosine similarity 1.vector 2.cosine value
        val cosine_1 = page.map{
          q => (q._1, Vectorize(q._2, query_1))
        }.map{
          q => (q._1,Cosine_Value(q._2, weight_1))
        }

        val cosine_2 = page.map{
          q => (q._1, Vectorize(q._2, query_2))
        }.map{
          q => (q._1,Cosine_Value(q._2, weight_2))
        }

        val cosine_3 = page.map{
          q => (q._1, Vectorize(q._2, query_3))
        }.map{
          q => (q._1,Cosine_Value(q._2, weight_3))
        }

        //pre-normalized
        val pagerank = pageranks.map{
        l =>val pair1 = l.split("\t", 2)

                pair1(1).toFloat
        }.collect().toList
        //take max and min
        val maxnumber = pagerank.max
        val minnumber = pagerank.min

        //normalized
        val normalized_pageranks = pageranks.map{
          l => val pair2 = l.split("\t",2)
          (pair2(0),pair2(1).toFloat)
        }.map{
          l=> (l._1, (l._2 - minnumber)/(maxnumber-minnumber))
        }
        //   var normalized =  (rank - min)/(max- min)
        // val cos = vectors.map{
        //   q => (q._1,Cosine_Value(q._2, weight_1))
        // }

        val output_1 = cosine_1.join(normalized_pageranks).map{
          q=> (q._1, 0.6 * q._2._1 + 0.4 * q._2._2)
        }.sortBy(_._2,false).map{
        q => (q._1 + "\t" + q._2)
        }

        val output_2 = cosine_2.join(normalized_pageranks).map{
          q=> (q._1, 0.6 * q._2._1 + 0.4 * q._2._2)
        }.sortBy(_._2,false).map{
        q => (q._1 + "\t" + q._2)
        }

        val output_3 = cosine_3.join(normalized_pageranks).map{
          q=> (q._1, 0.6 * q._2._1 + 0.4 * q._2._2)
        }.sortBy(_._2,false).map{
        q => (q._1 + "\t" + q._2)
        }

        //print answer
        println('\n' + "query_1: "+ query_1 + '\n')
        output_1.take(10).foreach(println)
        // cosine_2.take(10).foreach(println)
        // normalized_pageranks.take(10).foreach(println)

        println('\n' + "query_2: "+ query_2 + '\n')
        output_2.take(10).foreach(println)

       println('\n' + "query_3: "+ query_3 + '\n')
        output_3.take(10).foreach(println)

 }

         def Vectorize(context: String, query:String): String={
          var query_split = query.trim().split("\\s+",2)
          var vector_1 = 0
          var vector_2 = 0
          
          for (word <- context.split("\\W+")){
            //x
            if (word.toLowerCase() == query_split(0).toLowerCase()){
              vector_1 = vector_1 + 1
            }
            //y
            else if (word.toLowerCase() == query_split(1).toLowerCase()){
              vector_2 = vector_2 + 1
            }
          }
          (vector_1,vector_2).toString()

        }

       

        def Cosine_Value(vector: String, weight: String): Double ={
          var vector_xy = vector.stripPrefix("(").stripSuffix(")").trim().split(",")
          var x = vector_xy(0).toFloat
          var y =vector_xy(1).toFloat
          var weight_vector_xy = weight.split(",")
          var weight_x = weight_vector_xy(0).toFloat
          var weight_y = weight_vector_xy(1).toFloat
          //cosine similarity
          var denominator = Math.sqrt((Math.pow(x,2)+Math.pow(y,2))*(Math.pow(weight_x,2)+Math.pow(weight_y,2)))
          var numerator = x*weight_x+y*weight_y
          numerator/denominator

        }

}


