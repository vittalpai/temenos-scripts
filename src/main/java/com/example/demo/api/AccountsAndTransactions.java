package com.example.demo.api;

import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import org.bson.Document;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
public class AccountsAndTransactions {

   static MongoClient mongoClient;
   static MongoDatabase db;

   static MongoCollection<Document> transactionCollection, balanceCollection;

   static {
       ConnectionString connectionString = new ConnectionString("mongodb+srv://<cluster>.com/?w=1");
       mongoClient = MongoClients.create(connectionString);
       db = mongoClient.getDatabase("ms_holdings");
       transactionCollection = db.getCollection("ms_holdings_transaction");
       balanceCollection = db.getCollection("ms_holdings_balance");
   }

   @PostMapping("/holding-add/{id}")
   String addDoc(@PathVariable(value = "id") String accountID) {
       try (ClientSession clientSession = mongoClient.startSession()) {
           clientSession.startTransaction();
           balanceCollection.insertOne(getBalance(accountID));
           transactionCollection.insertMany(getTransactions(accountID));
           clientSession.commitTransaction();
           return "Success";
       } catch (Exception e) {
           System.out.println("aborted ");
           e.printStackTrace();
           return "Failed";
       }
   }

   private Document getBalance(String accountID) {
       long balanceDate = System.currentTimeMillis() / 1000L;
       String key = "GB0010001-" + accountID + "-" + balanceDate;
       Document doc = new Document();
       doc.append("_id", key);
       doc.append("accountId",  "GB0010001-" + accountID);
       doc.append("availableBalance", new BigDecimal("13277943.94"));
       doc.append("balanceDate", balanceDate);
       doc.append("currencyId", "USD");
       doc.append("customerId",  accountID);
       doc.append("disbursedAmount", new BigDecimal("0"));
       doc.append("extensionData", new Document());
       doc.append("externalIndicator", false);
       doc.append("objectId", key);
       doc.append("onlineActualBalance",  new BigDecimal("13278000.94"));
       doc.append("openingDate", new Date());
       doc.append("outstandingBalance", new BigDecimal("0"));
       doc.append("processingTime", new Date());
       doc.append("productName", "AC");
       doc.append("sanctionedAmount", new BigDecimal("0"));
       doc.append("workingBalance", new BigDecimal("13277943.94"));
       return doc;
   }

   private  List<Document> getTransactions(String accountID) {
       List<Document> payload = new ArrayList<Document>();
       Long key = Long.parseLong("16184656945080000");
       for (int i = 0; i < 2; i++) {
           String ID = "GB0010001-" + accountID + "-" + key;
           Document doc = new Document();
           doc.append("_id", ID);
           doc.append("sortKey", key);
           doc.append("accountId", "GB0010001-" +accountID);
           doc.append("accountOfficerId", 1);
           doc.append("amountInAccountCurrency",  new BigDecimal("111"));
           doc.append("amountInEventCurrency",  new BigDecimal("-129"));
           doc.append("bookingDate", new Date());
           doc.append("categorisactionId", 130);
           doc.append("chequeNumber", "");
           doc.append("currency", "USD");
           doc.append("customerId", accountID);
           doc.append("customerReference", "FT21105YDXJT");
           doc.append("extensionData", new Document());
           doc.append("externalIndicator", false);
           doc.append("externalReference", "");
           doc.append("narrative", "");
           doc.append("objectId", ID);
           doc.append("processingDate", new Date());
           doc.append("recordId", "197453644225786.030004");
           doc.append("runningBalance",  new BigDecimal("0"));
           doc.append("transactionAmount",  new BigDecimal("111"));
           doc.append("transactionReference", "FT21105YDXJT");
           doc.append("valueDate", new Date());
           payload.add(doc);
           key = key + 1000000000;
       }
       return payload;
   }

}