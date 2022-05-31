package com.example.demo.api;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonArray;
import java.util.Random;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;

@RestController
public class Transact {

   static MongoClient mongoClient;
   static MongoDatabase db;
   Random random = new Random();

   static MongoCollection<Document> transactionCollection, balanceCollection;
   Calendar today = Calendar.getInstance();
   long unixTimestamp = today.toInstant().getEpochSecond();
   FindOneAndReplaceOptions options = new FindOneAndReplaceOptions().upsert(true);
   Bson filter = eq("balanceDate", unixTimestamp);

   static {
       ConnectionString connectionString = new ConnectionString("mongodb+srv://<cluster>.com/?w=1");
       mongoClient = MongoClients.create(connectionString);
       db = mongoClient.getDatabase("ms_holdings");
       transactionCollection = db.getCollection("ms_holdings_transaction");
       balanceCollection = db.getCollection("ms_holdings_balance");
   }

   @PostMapping("/holding-transactions/{id}")
   String addTransaction(@PathVariable(value = "id") int id) {
       String accountID = String.format("GB0010001-%010d", id);
       try (ClientSession clientSession = mongoClient.startSession()) {
           clientSession.startTransaction();
           transactionCollection.insertOne(getTransaction(accountID, unixTimestamp));
           balanceCollection.findOneAndReplace(filter, getBalance(accountID, unixTimestamp), options);
           clientSession.commitTransaction();
           return "Success";
       } catch (Exception e) {
           System.out.println("aborted ");
           e.printStackTrace();
           return "Failed";
       }
   }

   private Document getTransaction(String accountID, long unixTimestamp) {

       Document doc = new Document();
       doc.append("sortKey", unixTimestamp);
       doc.append("currency", "USD");
       doc.append("recordId", "197767456438488.000001");
       doc.append("accountId", accountID);
       doc.append("narrative", "");
       doc.append("valueDate", "2021-04-15T00:00:00.000+0000");
       doc.append("customerId", accountID);
       doc.append("bookingDate", "2021-04-15T00:00:00.000+0000");
       doc.append("processingDate", "2021-04-15T00:00:00.000+0000");
       doc.append("runningBalance", 23908.36);
       doc.append("accountOfficerId", 39);
       doc.append("categorisactionId", 213);
       doc.append("customerReference", "FT2110500IF3");
       doc.append("externalIndicator", false);
       doc.append("externalReference", "");
       doc.append("transactionAmount",  random.nextInt(1000));
       doc.append("transactionReference", "FT2110500IF3");
       doc.append("amountInAccountCurrency", -100.00);
       return doc;
   }

   private Document getBalance(String accountID, Long balanceDate) {
       Document doc = new Document();
       doc.append("accountId", accountID);
       doc.append("currencyId", "USD");
       doc.append("customerId", accountID);
       doc.append("balanceDate", balanceDate);
       doc.append("openingDate", "2021-03-23T00:00:00.000+0000");
       doc.append("productName", "AC");
       doc.append("processingTime", "2022-02-23T14:44:48.970+0000");
       doc.append("workingBalance", 50200);
       doc.append("disbursedAmount", 0);
       doc.append("availableBalance", random.nextInt(1000));
       doc.append("sanctionedAmount", 0);
       doc.append("externalIndicator", false);
       doc.append("onlineActualBalance", 50200);
       doc.append("outstandingBalance", 0);
       return doc;
   }

}