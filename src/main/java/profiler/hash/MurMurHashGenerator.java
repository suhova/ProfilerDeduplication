package profiler.hash;


import static com.google.common.hash.Hashing.murmur3_128;

public class MurMurHashGenerator extends HashGeneratorWithTimer {
    @Override
    public String getHash(String value) {
        return String.valueOf(murmur3_128().hashUnencodedChars(value).asLong());
    }

    @Override
    public String getHashName() {
        return "murmur";
    }
}
