package deduplicator.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sha512HashGenerator extends HashGeneratorWithTimer {
    @Override
    public String getHash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-512");
            md.update(value.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            return new String(digest);
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getHashName() {
        return "sha512";
    }
}
