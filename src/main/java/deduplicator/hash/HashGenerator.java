package deduplicator.hash;

public interface HashGenerator {
    String getHash(String value);

    String getHashName();
}
