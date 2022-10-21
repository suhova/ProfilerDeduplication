package deduplicator.service;

import deduplicator.db.DbClient;
import deduplicator.hash.HashGeneratorWithTimer;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class WriterService {
    private final HashGeneratorWithTimer hashGenerator;
    private final String hashedPath;

    private long timeOfHashedDataWriting = 0;
    private int uniqueValues = 0;
    private int duplicates = 0;

    public WriterService(HashGeneratorWithTimer hashGenerator, String hashedPath) {
        this.hashGenerator = hashGenerator;
        this.hashedPath = hashedPath;
    }

    public void startWriting(int blockSize, int maxPositionInFile, String separator, String dataPath, DbClient client) {
        Set<String> uniqueValues = new HashSet<>();
        StringBuilder stringBuilderHash = new StringBuilder();

        int file = 0;
        int position = 0;
        try (FileInputStream is = new FileInputStream(dataPath)) {
            byte[] chunk = new byte[blockSize];
            while (is.read(chunk) != -1) {
                String data = new String(chunk);
                uniqueValues.add(data);
                long t1 = System.nanoTime();
                String hash = hashGenerator.getHash(data);
                boolean isHashExists = client.isElementPresent(hash, hashGenerator.getHashName());
                if (isHashExists) {
                    stringBuilderHash.append(hash).append(separator).append("\n");
                    client.updateHashCounter(hash, hashGenerator.getHashName());
                    duplicates++;
                } else {
                    stringBuilderHash.append(data).append(separator).append("\n");
                    client.writeHash(hash, file, position, hashGenerator.getHashName());
                }
                hashGenerator.addTime(System.nanoTime() - t1);
                position++;
                if (position > maxPositionInFile) {
                    position = 0;
                    writeToFile(stringBuilderHash.toString(), file);
                    stringBuilderHash = new StringBuilder();
                    file++;
                }
            }
            if (stringBuilderHash.length() != 0){
                writeToFile(stringBuilderHash.toString(), file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.uniqueValues = uniqueValues.size();
    }

    private void writeToFile(String hashData, int file) {
        long t1 = System.nanoTime();
        writeDataToFile(hashData, file, hashedPath + hashGenerator.getHashName() + "/");
        timeOfHashedDataWriting += System.nanoTime() - t1;
    }

    private static void writeDataToFile(String data, int file, String dir) {
        try (PrintWriter bw = new PrintWriter(new BlockLZ4CompressorOutputStream(new FileOutputStream(dir + file)))) {
            bw.write(data);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public long getTimeOfHashedDataWriting() {
        return timeOfHashedDataWriting;
    }

    public int getUniqueValues() {
        return uniqueValues;
    }

    public int getDuplicates() {
        return duplicates;
    }
}



