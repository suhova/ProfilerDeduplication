package deduplicator.hash;

public enum HashGenerators {
    MD5(new Md5HashGenerator()),
    MURMUR(new MurMurHashGenerator()),
    SHA512(new Sha512HashGenerator());
    public HashGeneratorWithTimer generator;

    HashGenerators(HashGeneratorWithTimer generator) {
        this.generator = generator;
    }
}
