package com.example.demo.api;

import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.web.bind.annotation.*;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.ConnectionString;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class Holding {

    static MongoClient mongoClient, mongoClient1;
    static MongoDatabase db, db1;
    Long fromDate = Long.parseLong("11184656945080000");
    Long toDate = Long.parseLong("96184656945080000");

    static MongoCollection<Document> transactionCollection, balanceCollection, transactionCollection1, balanceCollection1;

    static {
        ConnectionString connectionString = new ConnectionString("mongodb+srv:///<cluster>.com/?readPreference=secondary&maxPoolSize=200");
        mongoClient = MongoClients.create(connectionString);
        db = mongoClient.getDatabase("ms_holdings");
        transactionCollection = db.getCollection("ms_holdings_transaction");
        balanceCollection = db.getCollection("ms_holdings_balance");

        ConnectionString connectionString1 = new ConnectionString("mongodb+srv://<cluster>.com/?w=1&maxPoolSize=200");
        mongoClient1 = MongoClients.create(connectionString1);
        db1 = mongoClient1.getDatabase("ms_holdings");
        transactionCollection1 = db1.getCollection("ms_holdings_transaction");
        balanceCollection1 = db1.getCollection("ms_holdings_balance");
    }

    @PutMapping("/holding-balance/{id}")
    String updateBalance(@PathVariable(value = "id") int id) {
        String accountID = String.format("GB0010001-%010d", id);
        Document query = new Document().append("accountId", accountID);
        Bson updates = Updates.combine(
                Updates.set("availableBalance", new BigDecimal("2132131320")),
                Updates.set("onlineActualBalance", new BigDecimal("231321213321")),
                Updates.set("processingTime", new Date()));
        try (ClientSession clientSession = mongoClient1.startSession()) {
            clientSession.startTransaction();
            balanceCollection1.updateOne(query, updates);
            transactionCollection1.insertOne(getTransactions(id));
            clientSession.commitTransaction();
            return "Success";
        } catch (Exception e) {
            System.out.println("aborted ");
            e.printStackTrace();
            return "Failed";
        }
    }

    @GetMapping("/holding-balance/{id}")
    String fetchBalance(@PathVariable(value = "id") int id) {
        try {
            String accountID = String.format("GB0010001-%010d", id);
            Document doc = balanceCollection.find(new Document("accountId", accountID)).first();
            if (doc != null) {
                return "Success";
            }
        } catch (Exception e) {
            System.out.println("aborted ");
            e.printStackTrace();
        }
        return "Failed";
    }

    @GetMapping("/holding-transactions/{id}")
    String fetchTransactions(@PathVariable(value = "id") int id) {
        try {
            BasicDBObject searchQuery = new BasicDBObject();
            String accountID = String.format("GB0010001-%010d", id);
            searchQuery.put("accountId", accountID);
            searchQuery.put("sortKey", BasicDBObjectBuilder.start("$gte", fromDate).add("$lte", toDate).get());
            FindIterable<Document> doc = transactionCollection.find(searchQuery).limit(10);
            if (doc != null) {
                return doc.first().toJson();
            }
        } catch (Exception e) {
            System.out.println("aborted ");
            e.printStackTrace();
        }
        return "Failed";
    }

    private Document getTransactions(int accountID) {
        int key = ThreadLocalRandom
                .current()
                .nextInt(100, 1000000000);
        String ID = "GB0010001-" + accountID + "-" + key;
        Document doc = new Document();
        doc.append("_id", ID);
        doc.append("sortKey", key);
        doc.append("accountId", "GB0010001-" + accountID);
        doc.append("accountOfficerId", 1);
        doc.append("amountInAccountCurrency", new BigDecimal("111"));
        doc.append("amountInEventCurrency", new BigDecimal("-129"));
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
        doc.append("runningBalance", new BigDecimal("0"));
        doc.append("transactionAmount", new BigDecimal("111"));
        doc.append("transactionReference", "FT21105YDXJT");
        doc.append("valueDate", new Date());
//            payload.add(doc);
//            key = key + 1000000000;
        // }
        return doc;
    }

}