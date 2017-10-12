import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.StringBuilder

object PreProc {
    def main(args: Array[String]) {
        
        val inputfile = "/Users/yuanyaozhang/Desktop/WebSearch/USB/spark_disk/Download_and_Clean_Wikipedia/src/main/scala/enwiki-latest-pages-articles-multistream.xml"
        val outputfile = new PrintWriter(new File("output.txt")) 
        var a_output_line = new StringBuilder
        
        // write your code to extract content in every <page> .... </page> 
        // write each of that into one line in your output file
        var pagebegin = false
        var pagecount = 0

        for (inputline <- Source.fromFile(inputfile).getLines()) {
            var inputline_clean = inputline.trim
            //line start with <page>
            if(inputline_clean == "<page>"){
                a_output_line.clear
                a_output_line.append(inputline_clean)
                pagebegin = true
                pagecount+=1
            }
            //line end with </page>
            else if(inputline_clean == "</page>"){
                a_output_line.append(inputline_clean)
                outputfile.println(a_output_line)
                pagebegin = false
            }
            //line in between
            else{
                if(pagebegin == true){
                    a_output_line.append(inputline_clean)
                }
                    
            }
        }
        outputfile.close 
        println("total pages number: "+ pagecount)
    }
}

