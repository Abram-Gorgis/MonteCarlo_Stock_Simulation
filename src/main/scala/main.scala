

import java.io.{BufferedWriter, File, FileOutputStream, InputStreamReader, OutputStreamWriter}

import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object main {

  def main(args: Array[String]) {
   // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    val Log:Logger = LoggerFactory.getLogger("main")
    //used for sending and receiving files
    val region = Regions.US_EAST_1
    val Client = AmazonS3ClientBuilder.standard().withRegion(region).build()

    conf.setAppName("Monte")
    val sc = new SparkContext(conf)
    Log.info("Got spark context")

    //get config file
    val ObjectRequest = new GetObjectRequest("stockdatahw3", "config.conf")
    val configureObj = Client.getObject(ObjectRequest)
    val configData = configureObj.getObjectContent()
    val reader = new InputStreamReader(configData)
    val configure = ConfigFactory.parseReader(reader)
    Log.info("Got Config file from s3n")
    //get the starting array of funds and stocks
    val funds:Array[Double] = configure.getString("startingStocks.funds").split(",").map(_.toDouble)
    val stocks:Array[String]= configure.getString("startingStocks.stocks").split(",")
    Log.info("Split data for initial simulation")

    //remove dates row from file and insert into rdd
    val rdd = sc.textFile(configure.getString("settings.inputName")).flatMap(x=>{
      if(x.contains("date"))
        {
          List()
        }
      else
        List(x)
    })

    //make into tuple of stockname -> all doubles of changes in stocks of 20 years
    val mapRDD = rdd.map(line=>
    {
        val string = line.split("\\s+")
        val doubles = string.drop(1).map(x => x.toDouble)
        (string(0), doubles)
    })


    //make into map
    val newMap = mapRDD.collectAsMap()


    //map will be used by all nodes so broadcast it instead of constantly resending
   val z= sc.broadcast(newMap)

    print(z.value)
    Log.info("Broadcasting map to nodes")




    //create rdd of doubles of ending funds of simulation sorted
    val dataRdd=  sc.parallelize(1 to configure.getInt("settings.numberOfSimulations"))
      .map(i=>playSession(funds,stocks,configure.getInt("settings.numberOfDays"),z.value)).sortBy(x=>x,true,1)

    //get percentiles at different increments
    val percentiles = List(0.05, 0.25, 0.5, 0.75, 0.95).map(i =>
      dataRdd.zipWithIndex.map(_.swap).lookup((i * dataRdd.count).toLong).head)

    //debugging
    percentiles.foreach(println)







    //create a file to later send to aws S3
    val writer =
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output.txt")))
    // Write investment percentiles to file

    writer.write("Percentiles\n")
    writer.write("5th\t\t25th\t\t50th\t\t75th\t\t95th\n")
    percentiles.foreach(x => writer.write("%.2f".format(x).toString()+"\t"))
    writer.write("\n\n" +
      "Mean: ")
    writer.write((dataRdd.mean().toString))
    writer.write("\n" +
      "Std deviation: ")
    writer.write((dataRdd.stdev().toString))
    writer.close()

    Log.info("Finished writing to file")

    //send created file to aws
    val request = new PutObjectRequest("outputstocksdata", "output.txt", new File("output.txt"))
    val metadata = new ObjectMetadata
    metadata.setContentType("plain/text")
    request.setMetadata(metadata)
    Client.putObject(request)

    Log.info("Finished sending file to s3n")


  }


  //random stock simulation
  def playSession(startingFund: Array[Double], stocks: Array[String], numberOfDays: Int,map: scala.collection.Map[String, Array[Double]]):
  Double = {

    val r: scala.util.Random = scala.util.Random
    val allStocks = map.keySet.toList


    // Initialize values
    var (currentFund, currentStocks, currentGame) = (startingFund, stocks.toList, 1)

    // Keep playing until number of games is reached or funds run out
    while (currentGame <= numberOfDays) {


      //get random multiplier from list of that stock and applied that to proper stock
      var arrayFunds:Array[Double]=Array()
      currentStocks.foreach(X=>
        arrayFunds :+= (map(X)(r.nextInt(map(X).length)))/100.0
      )



      //i really couldnt think of a better way to sell stocks and get new stocks
      var newFunds:Array[Double]=Array()
      var newStocks:Array[String]=Array()
      var stocksToAdd:Set[String]=Set()
      var FundsToAdd:Double=0.0

      for(i<-0 to currentFund.length-1)
        {
          currentFund(i)=(currentFund(i)+currentFund(i)*arrayFunds(i))
          if(arrayFunds(i)*100>(0.04))//sell that stock if this happens
          {
            val numberOfNewStocks:Int = r.nextInt(3)+1//we want 1-3 new stocks
            for(i<-0 to numberOfNewStocks)//for each number pick a new stock
              {
                stocksToAdd += allStocks(r.nextInt(allStocks.length))//add to set incase we randomly chose the same stock twice
              }
            FundsToAdd += currentFund(i)//add how much we have invested in the stock we sold to our savings that we will distribute later back to the new stocks

          }
          else//if we arent selling just save stocks and funds of that stock
            {
              newFunds:+=currentFund(i)
              newStocks:+=currentStocks(i)
            }
        }

      //for each stock in our set we add it to our new stocks and new funds we generate
      stocksToAdd.foreach(x=>
      {
        if(!newStocks.contains(x))
          {

            newStocks:+=x
            newFunds :+=(FundsToAdd/stocksToAdd.size)
          }
        else
          {
            newFunds(newStocks.indexOf(x))= newFunds(newStocks.indexOf(x)) +(FundsToAdd/stocksToAdd.size)

          }

      })

      currentFund = newFunds
      currentStocks = newStocks.toList



      currentGame+=1
    }

    (currentFund.sum)

  }


}