import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

/**
 * MongoDB Query logic
 * @author nhuluong
 */
public class MongoDB {
    private MongoCollection<Document> coll;

    public MongoDB(String host, int port, String dbname, String collname) {
        coll = connect(host, port,dbname, collname);
    }

    /**
     * Establish connection to database
     * @param host host
     * @param port port
     * @param dbName dbName
     * @param collectionName collectionName
     * @return MongoCollection
     */
    public MongoCollection<Document> connect(String host, int port, String dbName, String collectionName) {
        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection<Document> coll = db.getCollection(collectionName);
        System.out.println("Connected To Host: " + host + " Port: " + port + " DB: " + dbName + " Collection: " + collectionName);
        return coll;
    }

    /**
     * Get the average price by country
     */
    public void averagePriceByCountry() {
        Block<Document> printBlock = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        coll.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.country_code")
                        .append("price", new Document("$avg", "$fields.price"))),
                new Document("$project", new Document("_id", 0)
                        .append("Country", "$_id")
                        .append("Average Price Per Day", "$price")
                ))).forEach(printBlock);

    }

    /**
     * Not being used, just for immediate report
     */
    public void modifyData() {
        Document query = new Document();
        query.append("fields.country_code","IE");
        Document setData = new Document();
        setData.append("fields.price", 10);
        Document update = new Document();
        update.append("$inc", setData);
        coll.updateMany(query, update);
        averagePriceByCountry();
    }

}
//REFERENCE
//        collection.aggregate(
//                Arrays.asList(Aggregates.group(
//                        "$fields.country_code", new BsonField("Average Price Per Day",
//                                new BsonDocument("$avg", new BsonString("$fields.price")))))
//        ).forEach(printBlock);
