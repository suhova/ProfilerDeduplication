package deduplicator.service;

import deduplicator.db.DbClient;
import deduplicator.hash.HashGeneratorWithTimer;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorOutputStream;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class WriterService {
    private final HashGeneratorWithTimer hashGenerator;
    private final String hashedPath;
    private final String originalPath;

    private long timeOfOriginalDataWriting = 0;
    private long timeOfHashedDataWriting = 0;
    private int uniqueValues = 0;

    public WriterService(HashGeneratorWithTimer hashGenerator, String hashedPath, String originalPath) {
        this.hashGenerator = hashGenerator;
        this.hashedPath = hashedPath;
        this.originalPath = originalPath;
    }

    public void startWriting(int blockSize, int maxPositionInFile, String separator, String dataPath, DbClient client) {
        Set<String> uniqueValues = new HashSet<>();
        StringBuilder stringBuilderHash = new StringBuilder();
        StringBuilder stringBuilderOriginal = new StringBuilder();

        int file = 0;
        int position = 0;
        try (FileInputStream is = new FileInputStream(dataPath)) {
            byte[] chunk = new byte[blockSize];
            while (is.read(chunk) != -1) {
                String data = new String(chunk);
                uniqueValues.add(data);
                stringBuilderOriginal.append(data);
                long t1 = System.nanoTime();
                String hash = hashGenerator.getHash(data);
                boolean isHashExists = client.isElementPresent(hash, hashGenerator.getHashName());
                if (isHashExists) {
                    stringBuilderHash.append(hash).append(separator).append("\n");
                    client.updateHashCounter(hash, hashGenerator.getHashName());
                } else {
                    stringBuilderHash.append(data).append(separator).append("\n");
                    client.writeHash(hash, file, position, hashGenerator.getHashName());
                }
                hashGenerator.addTime(System.nanoTime() - t1);
                position++;
                if (position > maxPositionInFile) {
                    position = 0;
                    writeToFile(stringBuilderOriginal.toString(), stringBuilderHash.toString(), file);
                    file++;
                }
            }
            writeToFile(stringBuilderOriginal.toString(), stringBuilderHash.toString(), file);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.uniqueValues = uniqueValues.size();
    }

    private void writeToFile(String data, String hashData, int file) {
        long t1 = System.nanoTime();
        writeDataToFile(data, file, originalPath);
        timeOfOriginalDataWriting += System.nanoTime() - t1;
        t1 = System.nanoTime();
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

    public long getTimeOfOriginalDataWriting() {
        return timeOfOriginalDataWriting;
    }

    public long getTimeOfHashedDataWriting() {
        return timeOfHashedDataWriting;
    }

    public int getUniqueValues() {
        return uniqueValues;
    }
}



