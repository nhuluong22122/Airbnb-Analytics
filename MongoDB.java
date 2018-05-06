import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

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
     * Get the average price by country
     */
    public void findAveragePriceByCountry() {
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
     * Find oldest listings
     */
    public void findOldestListings(){
        //db.ratings.find({}, {"_id":0, "fields.host_since": 1, "fields.city":1}).sort({"fields.host_since":1}).limit(20)
        FindIterable<Document> iter = coll.find()
                .limit(20)
                .sort(Sorts.ascending("fields.host_since"))
                .projection(Projections.fields(
                        Projections.include("fields.listing_url","fields.host_name", "fields.host_since", "fields.city"),
                        Projections.excludeId()));

        for (Document doc : iter) {
            doc = (Document) doc.get("fields");
            System.out.println("Listing url: " + doc.get("listing_url")
                    + " | Name: " + doc.get("host_name")
                    + " | Host Since: " + doc.get("host_since")
                    + " | City: " + doc.get("city"));
        }
    }

    public void findHighestReviewPerMonth(){

        FindIterable<Document> iter = coll.find()
                .limit(20)
                .sort(Sorts.descending("fields.reviews_per_month"))
                .projection(Projections.fields(
                        Projections.include("fields.reviews_per_month","fields.number_of_reviews","fields.listing_url", "fields.city", "fields.country"),
                        Projections.excludeId()));

        for (Document doc : iter) {
            doc = (Document) doc.get("fields");
            System.out.println("Reviews Per Month: " + doc.get("reviews_per_month")
                            + " | Total Reviews: " + doc.get("number_of_reviews")
                            + " | Listing Url: " + doc.get("listing_url")
                            + " | Location: " + doc.get("city") + ", " + doc.get("country"));
        }
    }
    /**
     * Find listings based on address
     * @param: zipcode zipcode
     */
    public void findListingsBasedOnZipcode(String zipcode) {
        try {
            //db.ratings.find({geometry:{ $near:{$geometry: { type: "Point", coordinates: [ -73.9667, 40.78 ] },$minDistance: 1000,$maxDistance: 5000}}}, {"fields.city": 1} )
            String API_KEY = "AIzaSyAzC9QtvwTkFeHkLPK55VG_VIXFmDj4rrc";
            String PLACES_API_BASE = "https://maps.googleapis.com/maps/api/geocode/json?";

            StringBuilder sb = new StringBuilder(PLACES_API_BASE);
            sb.append("&address=" + URLEncoder.encode(zipcode, "utf8"));
            sb.append("&key=" + API_KEY);

            URL url = new URL(sb.toString());
            System.out.println(sb.toString());

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder jsonResults = new StringBuilder();
            int read;
            char[] buff = new char[1024];
            while ((read = br.read(buff)) != -1) {
                jsonResults.append(buff, 0, read);
            }

            BsonDocument doc = BsonDocument.parse(jsonResults.toString());
            List<BsonValue> a = doc.get("results").asArray().getValues();
            BsonDouble lat;
            BsonDouble lng;
            for (BsonValue v : a){
                lat = v.asDocument().get("geometry").asDocument().get("location").asDocument().get("lat").asDouble();
                lng = v.asDocument().get("geometry").asDocument().get("location").asDocument().get("lng").asDouble();
                System.out.println(lat + " " + lng);

                Point refPoint = new Point(new Position( lng.doubleValue(), lat.doubleValue()));

                FindIterable<Document> iter = coll.find(Filters.near("geometry", refPoint , 5001.0,1000.0))
                        .limit(20)
                        .projection(Projections.fields(
                                Projections.include("fields.listing_url","fields.name","fields.city"),
                                Projections.excludeId()));

//                DistinctIterable<String> iter = coll.distinct("fields.listing_url",String.class).filter(Filters.near("geometry", refPoint , 2001.0,1000.0));
                for (Document document : iter) {
                    document = (Document) document.get("fields");
                    System.out.println("Listing URL: " + document.get("listing_url")
                                        + " | Name: " + document.get("name")
                                        + " | City: " + document.get("city"));
                }

            }





        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
        findAveragePriceByCountry();
    }

}
//REFERENCE
//        collection.aggregate(
//                Arrays.asList(Aggregates.group(
//                        "$fields.country_code", new BsonField("Average Price Per Day",
//                                new BsonDocument("$avg", new BsonString("$fields.price")))))
//        ).forEach(printBlock);
