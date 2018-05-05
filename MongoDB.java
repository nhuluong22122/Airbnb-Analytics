import com.mongodb.Block;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import javax.print.Doc;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * MongoDB Query logic
 * @author nhuluong
 */
public class MongoDB {
    private MongoCollection<Document> coll;
    Block<Document> printBlock; // for testing purposes

    public MongoDB(String host, int port, String dbname, String collname) {
        coll = connect(host, port,dbname, collname);
        printBlock = new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
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
     *
     */
    public void findOldestHost(){
        //db.ratings.find({}, {"_id":0, "fields.host_since": 1, "fields.city":1}).sort({"fields.host_since":1}).limit(20)
        FindIterable<Document> iter = coll.find()
                .limit(20)
                .sort(Sorts.ascending("fields.host_since"))
                .projection(Projections.fields(
                        Projections.include("fields.listing_url","fields.host_name", "fields.host_since", "fields.city"),
                        Projections.excludeId()));

        for (Document doc : iter) {
            Document s = (Document) doc.get("fields");
            System.out.println("Listing url: " + s.get("listing_url") + " | Name: " + s.get("host_name") + " | Host Since: " + s.get("host_since") + " | City: " + s.get("city"));
        }
    }
    /**
     * Get the average price by country
     */
    public void averagePriceByCountry() {
        //db.ratings.aggregate([{$group: {_id: "fields.country_code", price: {$avg:"$fields.price"}}},
        // {$project: {_id:0, "Country":"$_id", "Average Price Per Day": {$avg: "$price"}}}]);
        AggregateIterable<Document> iter = coll.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.country_code")
                        .append("price", new Document("$avg", "$fields.price"))),
                new Document("$project", new Document("_id", 0)
                        .append("Country", "$_id")
                        .append("Average Price Per Day", "$price")
                )));

        for (Document doc : iter) {
            System.out.println("Country: " + doc.get("Country") + " | Average Price Per Day: " + doc.get("Average Price Per Day"));
        }
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
