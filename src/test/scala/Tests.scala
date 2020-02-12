import java.io.{File, InputStreamReader}

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import com.typesafe.config.ConfigFactory
import org.junit.Assert._
import org.junit.Test
import org.junit.Before


class Tests {


  @Test def verifysplitsStocks()
  {
    val testString ="MSFT,APPL"
    assertTrue(testString.split(",").size==2)
    val funding = "20000.01,40000.00"
    val fundingD = funding.split(",").map(_.toDouble)

    assertTrue(fundingD.length==2)
    assertTrue(fundingD(0)==20000.01)
    assertTrue(fundingD(1)==40000.00)

  }

  @Test def checkMathOnStocks(): Unit =
  {
    val increase = 0.01
    val stockvalue = 100.0
    assertTrue((stockvalue+stockvalue*increase)==101.00)

  }

  @Test def testAwsConf(): Unit ={
    val region = Regions.US_EAST_1
    val Client = AmazonS3ClientBuilder.standard().withRegion(region).build()
    val ObjectRequest = new GetObjectRequest("stockdatahw3", "config.conf")
    val configureObj = Client.getObject(ObjectRequest)
    val configData = configureObj.getObjectContent()
    val reader = new InputStreamReader(configData)
    val configure = ConfigFactory.parseReader(reader)
    assertFalse(configure.isEmpty)
  }

  @Test def checkPickingRandomStock():Unit ={

    val map = Map("MSFT"->Array(4.5,30.5,6.9))
    val r: scala.util.Random = scala.util.Random
    for(i<-0 to 100) {
      assertTrue(r.nextInt(map("MSFT").length) < 3)
    }
  }

  @Test def checkRandomStock(): Unit ={
    val map = Map("MSFT"->Array(4.5,30.5,6.9),"MSFT2"->Array(4.5,30.5,6.9),"MSFT2"->Array(4.5,30.5,6.9))
    val set = map.keySet.toList
    assertTrue(set.length==3)
  }

}
