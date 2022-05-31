package com.example.demo.api;

import com.mongodb.ConnectionString;
import com.mongodb.client.*;
import org.bson.Document;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@RestController
public class Party {

   static MongoClient mongoClient;
   static MongoDatabase db;

   static MongoCollection<Document> ms_party_party, ms_party_role, ms_party_partyrelationship, ms_party_partyidentifier, ms_party_partyassessment, ms_party_occupation, ms_party_observation, ms_party_groupcontacts, ms_party_employment, ms_party_addresses, ms_party_alternateidentity, ms_party_classification, ms_party_contactreferences;

   static {
       ConnectionString connectionString = new ConnectionString("mongodb+srv://<cluster>.com/?w=majority");

       mongoClient = MongoClients.create(connectionString);
       db = mongoClient.getDatabase("ms_party");
       ms_party_party = db.getCollection("ms_party_party");
       ms_party_addresses = db.getCollection("ms_party_addresses");
       ms_party_alternateidentity = db.getCollection("ms_party_alternateidentity");
       ms_party_classification = db.getCollection("ms_party_classification");
       ms_party_contactreferences = db.getCollection("ms_party_contactreferences");
       ms_party_employment = db.getCollection("ms_party_employment");
       ms_party_groupcontacts = db.getCollection("ms_party_groupcontacts");
       ms_party_observation = db.getCollection("ms_party_observation");
       ms_party_occupation = db.getCollection("ms_party_occupation");
       ms_party_partyassessment = db.getCollection("ms_party_partyassessment");
       ms_party_partyidentifier = db.getCollection("ms_party_partyidentifier");
       ms_party_partyrelationship = db.getCollection("ms_party_partyrelationship");
       ms_party_role = db.getCollection("ms_party_role");

   }

   @PostMapping("/party-add/{id}")
   String addDoc(@PathVariable(value = "id") String partyID) {
try (ClientSession clientSession = mongoClient.startSession()) {
//        try {
         clientSession.startTransaction();
           ms_party_party.insertOne(getParty(partyID));
           ms_party_addresses.insertOne(getAddresses(partyID));
           ms_party_alternateidentity.insertOne(getAlternateidentity(partyID));
           ms_party_classification.insertOne(getClassification(partyID));
           ms_party_contactreferences.insertOne(getContactreferences(partyID));
           ms_party_employment.insertOne(getEmployment(partyID));
           ms_party_groupcontacts.insertOne(getGroupcontacts(partyID));
           ms_party_observation.insertOne(getObservation(partyID));
           ms_party_occupation.insertOne(getOccupation(partyID));
           ms_party_partyassessment.insertOne(getPartyassessment(partyID));
           ms_party_partyidentifier.insertOne(getPartyidentifier(partyID));
           ms_party_partyrelationship.insertOne(getPartyrelationship(partyID));
           ms_party_role.insertOne(getRole(partyID));
           clientSession.commitTransaction();
           return "Success";
       } catch (Exception e) {
           System.out.println("aborted ");
           e.printStackTrace();
           return "Failed";
       }
   }

   private Document getParty(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("alias", "Rolf");
       doc.append("citizenship",  new ArrayList<String>());
       doc.append("cityOfBirth", "Newyork");
       doc.append("countryOfBirth", "US");
       doc.append("dateOfBirth", new Date());
       doc.append("dateOfDeath", new Date());
       doc.append("dateOfIncorporation", new Date());
       doc.append("defaultLanguage", "English");
       doc.append("entityName", "Temenos Group");
       doc.append("firstName", "Rolf");
       doc.append("gender", "Male");
       doc.append("incorporationCountry", "US");
       doc.append("lastName", "Gerling");
       doc.append("legalForm", "Listed Company");
       doc.append("maritalStatus", "Married");
       doc.append("middleName", "J");
       doc.append("nameOfIncorporationAuthority", "");
       doc.append("nationality",  new ArrayList<String>());
       doc.append("nickName", "Rolf");
       doc.append("noOfDependents", 4);
       doc.append("notificationOfDeath", new Date());
       doc.append("numberOfEmployees", 500);
       doc.append("objectId", partyID);
       doc.append("partyId", partyID);
       doc.append("organisationLegalType", "Legal");
       doc.append("otherRiskIndicator",  new ArrayList<String>());
       doc.append("partyLanguage",  new ArrayList<String>());
       doc.append("partyLifeCycle",  new ArrayList<String>());
       doc.append("partyStatus", "Prospect");
       doc.append("partyType", "Individual");
       doc.append("personPosition", new ArrayList<String>());
       doc.append("reasonForNoCitizenship", "Migration from another country");
       doc.append("residence", new ArrayList<String>());
       doc.append("startDate", new Date());
       doc.append("suffix", "B.E.");
       doc.append("title", "Mr");
       doc.append("vulnerability", new ArrayList<String>());
       return doc;
   }

   private Document getAddresses(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("phoneNo", "468787578");
       doc.append("primary", true);
       doc.append("addressType", "Legal");
       doc.append("businessKey", "AD22074SD5CG");
       doc.append("iddPrefixPhone", "+1");
       doc.append("softDeleteFlag", false);
       doc.append("communicationType", "Landline");
       doc.append("addressesReference", "AD22074SD5CG");
       doc.append("communicationNature", "Phone");
       return doc;
   }

   private Document getAlternateidentity(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("businessKey", "BackOfficeIdentifier");
       doc.append("identityType", "BackOfficeIdentifier");
       doc.append("extensionData", new ArrayList<String>());
       doc.append("identityNumber", "GB0010001-999999");
       doc.append("identitySource", "TransactT24");
       return doc;
   }

   private Document getClassification(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("businessKey", "T");
       doc.append("extensionData", new ArrayList<String>());
       doc.append("classificationCode", "TARGET-30");
       return doc;
   }

   private Document getContactreferences(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("jobTitle", "01");
       doc.append("startDate", new Date());
       doc.append("businessKey", "CR22066OYPPT");
       doc.append("softDeleteFlag", false);
       doc.append("contactPartyRef", "2206617685");
       doc.append("reliabilityType", "Confirmed");
       doc.append("contactAddressId", "AD22066DZYVN");
       doc.append("contactReferences", "CR22066OYPPT");
       doc.append("contactTypePurpose", "Primary");
       return doc;
   }

   private Document getEmployment(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("town", "Chennai");
       doc.append("type", "Casual Employee");
       doc.append("businessKey", "CR22066OYPPT");
       doc.append("country", "IN");
       doc.append("startDate", new Date());
       doc.append("streetName", "ADASDA");
       doc.append("businessKey", "EM22066RU3YF");
       doc.append("buildingName", "Taramani");
       doc.append("employerName", "Temenos Office");
       doc.append("softDeleteFlag", false);
       doc.append("postalOrZipCode", "600096");
       doc.append("primaryEmployment", true);
       doc.append("countrySubdivision", "IN-TN");
       doc.append("employerOfficePhone", "6374957977");
       doc.append("employmentReference", "EM22066RU3YF");
       doc.append("employerOfficePhoneIdd", "+91");
       doc.append("statutoryRequirementMet", false);
       return doc;
   }

   private Document getGroupcontacts(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("groupId", "GRP22073TDKZN");
       doc.append("startDate", new Date());
       doc.append("businessKey", "GCR22073RW0EO");
       doc.append("contactType", "Office");
       doc.append("softDeleteFlag", false);
       doc.append("groupContactRef", "GCR22073RW0EO");
       doc.append("contactReference", "CR22073H4YP4");
       return doc;
   }

   private Document getObservation(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("type", "Summary");
       doc.append("detail", "test");
       doc.append("source", "bfleck");
       doc.append("businessKey", "OB22066JA15U");
       doc.append("softDeleteFlag", false);
       doc.append("observationDate", new Date());
       doc.append("observationReference", "OB22066JA15U");
       return doc;
   }

   private Document getOccupation(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("businessKey", "Accountant");
       doc.append("occupationType", "Accountant");
       return doc;
   }

   private Document getPartyassessment(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("body", "Blacklist");
       doc.append("type", "Regulatory-Blacklist");
       doc.append("score", "");
       doc.append("reason", "Ext Integration");
       doc.append("status", "pass");
       doc.append("details", "pass");
       doc.append("businessKey", "Regulatory-Blacklist");
       doc.append("conclusionDate", new Date());
       return doc;
   }

   private Document getPartyidentifier(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("type", "Registration");
       doc.append("primary", false);
       doc.append("isExternal", false);
       doc.append("businessKey", "Registration");
       doc.append("identifierNumber", "1234567890");
       doc.append("isLegalIdentifier", false);
       return doc;
   }

   private Document getPartyrelationship(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("startDate", new Date());
       doc.append("relatedParty", "2206617685");
       doc.append("extensionData", new ArrayList<String>());
       doc.append("softDeleteFlag", false);
       doc.append("relationshipType", "Legal");
       doc.append("relationshipReference", "PR22066DN4B8");
       return doc;
   }

   private Document getRole(String partyID) {
       Document doc = new Document();
       doc.append("_id", partyID);
       doc.append("partyId", partyID);
       doc.append("status", "Active");
       doc.append("fromDate", new Date());
       doc.append("customership", new ArrayList<String>());
       doc.append("roleType", "Representative");
       doc.append("businessKey", "Representative");
       return doc;
   }

}
