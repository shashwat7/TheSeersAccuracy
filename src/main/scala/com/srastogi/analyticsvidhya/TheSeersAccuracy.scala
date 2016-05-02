package com.srastogi.analyticsvidhya

/**
 * Created by shashwat on 4/30/16.
 */

import java.util.{Calendar, Date}

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

case class trainingExample(val transaction_id: String,
                           val transaction_date: Date,
                           val store_id: String,
                           val number_of_emi: Int,
                           val purchased_in_sale: Boolean,
                           val var1: Int,
                           val var2: Int,
                           val var3: Int,
                           val client_id: String,
                           val gender: String,
                           val dob: Date,
                           val referred_friend: Boolean,
                           val sales_executive_id: String,
                           val sales_executive_category: String,
                           val lead_source_category: String,
                           val payment_mode: String,
                           val product_category: String,
                           val transaction_amount: Int)

object TheSeersAccuracy {
  def main(args: Array[String]) {

    // Setup Spark
    val conf = new SparkConf()
      .setAppName("The Seers Accuracy")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read data
    val trainingFile = "/home/shashwat/TheSeersAccuracy/src/main/resources/Train_seers_accuracy.csv"
    val dataSchema = StructType(Array(
      StructField("Transaction_ID", StringType, true),
      StructField("Transaction_Date", DateType, true),
      StructField("Store_ID", StringType, true),
      StructField("Number_of_EMI", IntegerType, true),
      StructField("Purchased_in_Sale", StringType, true),
      StructField("Var1", IntegerType, true),
      StructField("Var2", IntegerType, true),
      StructField("Var3", IntegerType, true),
      StructField("Client_ID", StringType, true),
      StructField("Gender", StringType, true),
      StructField("DOB", DateType , true),
      StructField("Referred_Friend", StringType, true),
      StructField("Sales_Executive_ID", StringType, true),
      StructField("Sales_Executive_Category", StringType, true),
      StructField("Lead_Source_Category", StringType, true),
      StructField("Payment_Mode", StringType, true),
      StructField("Product_Category", StringType, true),
      StructField("Transaction_Amount", IntegerType, true)
    ))

    // Read training data
    val dataDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(dataSchema)
      .option("mode","FAILFAST")
      .option("dateFormat","dd-MMM-yy")
      .load(trainingFile)

    dataDf.groupBy("Client_ID").agg({'':''})

    /*
    * Feature to be derived:
    * 1. How many times has the person shopped before this purchase?
    * */

//    val Array(trainigDataDf,testDataDf) = dataDf.randomSplit(Array(0.7,0.3))

    val dataRdd = dataDf.rdd
      .cache()

    val client_purchaseDate = dataRdd.groupBy(_.getString(8))
      .mapValues(listOfRow => listOfRow.map(row => row.getDate(1)))

    val dataWithAllClientDates = dataRdd.keyBy(_.getString(8))
      .join(client_purchaseDate)
      .map{case(client_id, (row,allPurchaseDates)) =>
        val purchaseDateOfThisTransaction = row.getDate(1)
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(row.getDate(1))
        cal.add(Calendar.YEAR,1)
        val purchaseDateOfThisTransactionPlus1Year = cal.getTime
        val numberOfPurchasesMadeBefore = allPurchaseDates.filter(dt => dt.before(purchaseDateOfThisTransaction)).size
        val numberOfPurchaseInNext1year = allPurchaseDates.filter(dt => dt.after(purchaseDateOfThisTransaction) && dt.before(purchaseDateOfThisTransactionPlus1Year)).size
        val repeatCustomer = if(numberOfPurchaseInNext1year > 0) 1 else 0
        (
          repeatCustomer,                               // label
          row.getDate(1),                               // Transaction Date
          row.getString(2),                               // Store ID
          row.getInt(3),                                // number of emi
          row.getString(4),                             // Purchase in sale
          row.getInt(5),                                // Var1
          row.getInt(6),                                // Var2
          row.getInt(7),                                // Var3
          row.getString(9),                             // Gender
          row.getDate(10),                              // DOB
          row.getString(11),                            // Refered Friend
          row.getString(12),
          row.getString(13),
          row.getString(14),
          row.getString(15),
          row.getString(16),
          row.getInt(17),                               // Transaction Amount
          numberOfPurchasesMadeBefore                   // Number of purchases made before
          )
      }.take(100).foreach(println)


  }
}
