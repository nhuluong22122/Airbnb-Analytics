import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
        //db.ratings.aggregate([{$group: {_id: "$fields.country_code", price: {$avg:"$fields.price"}}},
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
                    + "\n | Name: " + doc.get("host_name")
                    + "\n | Host Since: " + doc.get("host_since")
                    + "\n | City: " + doc.get("city"));
        }
    }

    /**
     * Find the listings and cities with the most Airbnb reviews
     */
    public void findMostAirbnbReviews(){
        //db.ratings.find({},{"fields.listing_url":1, "fields.name":1}).limit(100).sort({"fields.number_of_reviews": 1});
        FindIterable<Document> iter = coll.find()
                .limit(10)
                .sort(Sorts.descending("fields.number_of_reviews"))
                .projection(Projections.fields(
                        Projections.include("fields.listing_url","fields.name","fields.number_of_reviews", "fields.city", "fields.country"),
                        Projections.excludeId()));

        for (Document doc : iter) {
            doc = (Document) doc.get("fields");
            System.out.println("Listing url: " + doc.get("listing_url")
                    + "\n | Name: " + doc.get("name")
                    + "\n | Number of Reviews: " + doc.get("number_of_reviews")
                    + "\n | Location: " + doc.get("city") + " " + doc.get("country"));
        }

    }

    /**
     * Find statistic about the top Highest Reviews Per Month
     */
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
                            + "\n | Total Reviews: " + doc.get("number_of_reviews")
                            + "\n | Listing Url: " + doc.get("listing_url")
                            + "\n | Location: " + doc.get("city") + ", " + doc.get("country"));
        }
    }

    /**
     * Rank countries based on reviews
     */
    public void rankCountries(){
        //db.ratings.aggregate([{$group: {_id: "$fields.country", count: {$sum: 1},  amount: { $avg: { $multiply :
        // [ '$fields.review_scores_value', '$fields.review_scores_accuracy','$fields.review_scores_cleanliness',
        // '$fields.review_scores_location','$fields.review_scores_checkin','$fields.review_scores_communication']}}}},
        // {$project: {_id:0, "Country":"$_id", "Average Review": "$amount"}},{$sort:{“Average Review“:-1}}])
        List<String> srt_array = Arrays.asList("$fields.review_scores_value",
                "$fields.review_scores_accuracy","$fields.review_scores_cleanliness",
                "$fields.review_scores_location", "$fields.review_scores_checkin","$fields.review_scores_communication");
        AggregateIterable<Document> iter = coll.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.country")
                        .append("count", new Document("$sum", 1))
                        .append("average", new Document("$avg", new Document("$multiply",srt_array)))),
                new Document("$project", new Document("_id", 0)
                        .append("Country", "$_id")
                        .append("Number of Listings", "$count")
                        .append("Average Review", "$average")),
                new Document("$sort", new Document("Average Review", -1))));

        for (Document doc : iter) {
            System.out.println("Country: " + doc.get("Country")
                    + "\n | Number of Listings: " + doc.get("Number of Listings")
                    + "\n | Average Review: " + doc.get("Average Review"));
        }
    }

    /**
     * Find the top 10 cities with the most listings
     */
    public void findMostListings() {
        //db.ratings.aggregate([{$group: {_id: "$fields.city", totalcount: {$sum:"$fields.host_listings_count"}}},
        // {$project: {_id:0, "City":"$_id", "Total Count":"$totalcount"}},{$sort:{"Total Count":-1 }}]);
        AggregateIterable<Document> iter = coll.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$fields.city")
                        .append("count", new Document("$sum", "$fields.host_listings_count"))),
                new Document("$project", new Document("_id", 0)
                        .append("City", "$_id")
                        .append("Number of Listing", "$count")),
                new Document("$sort", new Document("Number of Listing", -1)),
                new Document("$limit", 10)));

        for (Document doc : iter) {
            System.out.println("City: " + doc.get("City")
                    + "\n | Number of Listings: " + doc.get("Number of Listing"));
        }
    }

    /**
     * Find the number of hosts and super hosts in different countries in 2017
     */
    public void findHosts(){
        //db.ratings.aggregate([{$group: {_id: "$fields.country", hosts: {$sum:"$fields.host"},
        // superhosts: { $cond: ["fields.host_is_superhost",1,0]}},
        // {$project: {_id:0, "Country":"$_id", "Hosts":"$hosts", "Superhosts":"$superhosts"}},{$sort:{"Host":-1,"Superhosts":-1}}]);
    }
    /**
     * Find listings based on address
     * @param: zipcode zipcode
     */
    public void findListingsBasedOnZipcode(String zipcode, double maxRange, double minRange) {
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
            BsonValue v = a.get(0);
            lat = v.asDocument().get("geometry").asDocument().get("location").asDocument().get("lat").asDouble();
            lng = v.asDocument().get("geometry").asDocument().get("location").asDocument().get("lng").asDouble();
            System.out.println(lat + " " + lng);

            Point refPoint = new Point(new Position( lng.doubleValue(), lat.doubleValue()));
            if(maxRange > 0){ // for query 13
                FindIterable<Document> iter = coll.find(Filters.near("geometry", refPoint , maxRange , minRange))
                        .limit(20)
                        .projection(Projections.fields(
                                Projections.include("fields.listing_url","fields.name","fields.city"),
                                Projections.excludeId()));
                for (Document document : iter) {
                    document = (Document) document.get("fields");
                    System.out.println("Listing URL: " + document.get("listing_url")
                            + "\n | Name: " + document.get("name")
                            + "\n | City: " + document.get("city"));
                }
            }
            else { // for query 14
                FindIterable<Document> iter = coll.find(Filters.near("geometry", refPoint , 100.0,0.0))
                        .limit(20)
                        .sort(Sorts.ascending("geometry"))
                        .projection(Projections.fields(
                                Projections.include("geometry","fields.listing_url","fields.name","fields.number_of_reviews","fields.price"),
                                Projections.excludeId()));
                for (Document document : iter) {
                    Document d2 = (Document) document.get("geometry");
                    String[] coor = d2.get("coordinates").toString().replace("[","").replace("]","").split(",");
                    double gap = distance(lat.doubleValue(), Double.parseDouble(coor[1]), lng.doubleValue() ,Double.parseDouble(coor[0]));
                    document = (Document) document.get("fields");
                    System.out.println("Listing URL: " + document.get("listing_url")
                            + "\n | Distance in Miles: " + (gap * 0.621) + " miles"
                            + "\n | Name: " + document.get("name")
                            + "\n | Price Per Night: " + document.get("price"));
                }
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Calculate distance between two points in latitude and longitude
     * @param lat1 1st latitude
     * @param lat2 2nd latitude
     * @param lon1 1st longtitude
     * @param lon2 2nd longtitude
     * @return distance between two points
     */
    public static double distance(double lat1, double lat2, double lon1,
                                  double lon2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
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
//                DistinctIterable<String> iter = coll.distinct("fields.listing_url",String.class).filter(Filters.near("geometry", refPoint , 2001.0,1000.0));
