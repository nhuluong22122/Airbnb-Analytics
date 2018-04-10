import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        System.out.println("-------------Welcome to Airbnb Analytics-------------");
        MongoCollection<Document> coll = connect("localhost", 27022, "airbnb", "ratings");
        averagePriceByCountry(coll);
//        modifyData(coll);
    }

    public static MongoCollection<Document> connect(String host, int port, String dbName, String collectionName) {
        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection<Document> coll = db.getCollection(collectionName);
        System.out.println("Connected To Host: " + host + " Port: " + port + " DB: " + dbName + " Collection: " + collectionName);
        return coll;
    }

    public static void averagePriceByCountry(MongoCollection<Document> collection) {
        Block<Document> printBlock = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.country_code")
                        .append("price", new Document("$avg", "$fields.price"))),
                new Document("$project", new Document("_id", 0)
                        .append("Country", "$_id")
                        .append("Average Price Per Day", "$price")
                ))).forEach(printBlock);

    }

    public static void modifyData(MongoCollection collection) {
        Document query = new Document();
        query.append("fields.country_code","IE");
        Document setData = new Document();
        setData.append("fields.price", 10);
        Document update = new Document();
        update.append("$inc", setData);
        collection.updateMany(query, update);
        averagePriceByCountry(collection);
    }
}
//        collection.aggregate(
//                Arrays.asList(Aggregates.group(
//                        "$fields.country_code", new BsonField("Average Price Per Day",
//                                new BsonDocument("$avg", new BsonString("$fields.price")))))
//        ).forEach(printBlock);

