package profiler.db;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

public class DbClient {
    public final MongoClient client;
    public final String DB_NAME = "profiler";

    public DbClient() {
        this.client = MongoClients.create("mongodb://localhost:27017/?compressors=zstd");
    }

    public void clearCollection(String collectionName){
        client.getDatabase(DB_NAME)
            .getCollection(collectionName)
            .deleteMany(new Document());
    }

    public void writeHash(String hash, String value, String collectionName){
        MongoCollection<Document> collection = client
            .getDatabase(DB_NAME)
            .getCollection(collectionName);
        try {
            collection.insertOne(new Document()
                .append("hash", hash)
                .append("value", value)
            );
        } catch (MongoWriteException e){
            if (e.getCode() != 11000){
                e.printStackTrace();
            }
        }
    }

    public String findByHash(String hash, String collectionName){
        return client.getDatabase(DB_NAME)
            .getCollection(collectionName)
            .find(new Document("hash", hash))
            .first()
            .getString("value");
    }
}