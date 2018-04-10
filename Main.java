import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DBObject;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;

import java.net.UnknownHostException;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.Set;


import static java.util.concurrent.TimeUnit.SECONDS;

public class Main {
    public static void main(String[] args){
        System.out.println("-------------Welcome to Airbnb Analytics-------------");
        MongoCollection coll = connect("localhost", 27022,"airbnb", "ratings");
        averagePriceByCountry(coll);
    }

    public static MongoCollection connect(String host, int port, String dbName, String collectionName){
        MongoClient mongoClient = new MongoClient(host,port);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(collectionName);
        System.out.println("Connected To Host: " + host + " Port: " + port + " DB: " + dbName + " Collection: " + collectionName);
        return coll;
    }

    public static void averagePriceByCountry(MongoCollection collection){
        Block<Document> printBlock = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.country_code")
                .append("price", new Document("$avg","$fields.price"))),
                new Document("$project",new Document("_id",0)
                                .append("Country", "$_id" )
                                .append("Average Price Per Day", "$price")
        ))).forEach(printBlock);

//        collection.aggregate(
//                Arrays.asList(Aggregates.group(
//                        "$fields.country_code", new BsonField("Average Price Per Day",
//                                new BsonDocument("$avg", new BsonString("$fields.price")))))
//        ).forEach(printBlock);

    }
}
