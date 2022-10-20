package deduplicator.db;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Updates;
import org.bson.Document;

public class DbClient {
    public final MongoClient client;
    public final String DB_NAME = "sabd";

    public DbClient() {
        this.client = MongoClients.create("mongodb://localhost:27017/?compressors=zstd");
    }

    public void clearCollection(String collectionName) {
        client.getDatabase(DB_NAME).getCollection(collectionName).drop();
        client.getDatabase(DB_NAME).createCollection(collectionName);
    }

    public void writeHash(String hash, int file, int position, String collectionName) {
        MongoCollection<Document> collection = client
            .getDatabase(DB_NAME)
            .getCollection(collectionName);
        try {
            collection.insertOne(new Document()
                .append("hash", hash)
                .append("file", file)
                .append("position", position)
                .append("count", 1)
            );
        } catch (MongoWriteException e) {
            if (e.getCode() != 11000) {
                e.printStackTrace();
            }
        }
    }

    public void updateHashCounter(String hash, String collectionName) {
        MongoCollection<Document> collection = client
            .getDatabase(DB_NAME)
            .getCollection(collectionName);
        int count = collection.find(new Document("hash", hash))
            .first()
            .getInteger("count");
        collection.updateOne(
            new Document().append("hash", hash),
            Updates.set("count", count + 1)
        );
    }

    public boolean isElementPresent(String hash, String collectionName) {
        return client.getDatabase(DB_NAME)
            .getCollection(collectionName)
            .find(new Document("hash", hash)).first() != null;
    }

    public int[] findByHash(String hash, String collectionName) {
        Document res = client.getDatabase(DB_NAME)
            .getCollection(collectionName)
            .find(new Document("hash", hash))
            .first();
        assert res != null;
        return new int[]{res.getInteger("file"), res.getInteger("position")};
    }

    public int getStats(String collection) {
        return client.getDatabase(DB_NAME)
            .runCommand(new Document("collStats", collection))
            .getInteger("storageSize");
    }
}