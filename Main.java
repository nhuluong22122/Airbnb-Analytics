import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Main {
    public static void main(String[] args){
        System.out.println("-------------Welcome to Airbnb Analytics-------------");
        connect(27022, "airbnb", "ratings");
    }

    public static void connect(int port, String dbName, String collectionName){
        try {
            MongoClient mongoClient = new MongoClient( "localhost" , port );
            DB db = mongoClient.getDB( dbName);
            DBCollection coll = db.getCollection(collectionName);
            DBCursor cursor = coll.find();
            int i = 0;
            try {
                while(cursor.hasNext() && i < 3) {
                    System.out.println(cursor.next());
                    i++;
                }
            } finally {
                cursor.close();
            }

        }
        catch (UnknownHostException e){
            System.out.print("Cannot connect to port");
        }
    }
}
