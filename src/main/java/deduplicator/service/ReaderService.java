package deduplicator.service;

import deduplicator.db.DbClient;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ReaderService {
    private long hashReadingTime = 0;
    private long originalReadingTime = 0;
    private int errorCount = 0;
    private final boolean calculateMistakes;

    public ReaderService(boolean calculateMistakes) {
        this.calculateMistakes = calculateMistakes;
    }

    public void read(String hashedPath, String hashName, DbClient client, String originalPath, String separator,
                     String warehousePath, String sourceFilePath, int blockSize) {
        File dir = new File(hashedPath);
        StringBuilder dataFromHash = new StringBuilder();
        for (File file : dir.listFiles()) {
            String fileName = file.getName();

            try (
                BufferedReader br = new BufferedReader(new InputStreamReader(new BlockLZ4CompressorInputStream(new FileInputStream(dir + "/" + fileName))));
                FileInputStream is = new FileInputStream(sourceFilePath)
            ) {
                if (calculateMistakes) {
                    byte[] chunk = new byte[blockSize];
                    while (br.ready()) {
                        String expected = "";
                        if (is.read(chunk) != -1) {
                            expected = new String(chunk);
                        }
                        long t1 = System.nanoTime();
                        String actual = getDataFromHash(br, separator, client, hashName);
                        hashReadingTime += System.nanoTime() - t1;
                        dataFromHash.append(actual);
                        if (!actual.equals(expected)) {
                            errorCount++;
                            System.out.println("ErrorCnt=" + errorCount + "\n Expected block: " + expected + ", actual: " + actual);
                        }
                    }
                } else {
                    long t1 = System.nanoTime();
                    while (br.ready()) {
                        dataFromHash.append(getDataFromHash(br, separator, client, hashName));
                    }
                    hashReadingTime += System.nanoTime() - t1;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
//            long t1 = System.nanoTime();
//            readOriginalDataFromFile(file.getName(), originalPath);
//            originalReadingTime += System.nanoTime() - t1;
        }
        writeToFile(warehousePath, dataFromHash.toString());
    }

    private String getDataFromHash(BufferedReader br, String separator, DbClient client, String hashName) throws IOException {
        String hash = readBlock(br, separator);
        if (client.isElementPresent(hash, hashName)) {
            int[] filePos = client.findByHash(hash, hashName);
            int f = filePos[0];
            int pos = filePos[1];
            return readHashedDataFromFile(f, hash, pos, separator);
        } else {
            return hash;
        }
    }

    private void writeToFile(String path, String data) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path + "/restored.csv", StandardCharsets.UTF_8))) {
            bw.write(data);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private String readOriginalDataFromFile(String fileName, String dir) {
        try (BufferedReader br = new BufferedReader(new FileReader(dir + fileName, StandardCharsets.UTF_8))) {
            StringBuilder res = new StringBuilder();
            while (br.ready()) {
                res.append(br.readLine());
            }
            return res.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String readHashedDataFromFile(int fileName, String dir, int position, String separator) {
        try (BufferedReader br = new BufferedReader(new FileReader(dir + fileName, StandardCharsets.UTF_8))) {
            int curPos = 0;
            while (curPos < position) {
                readBlock(br, separator);
                curPos++;
            }
            return readBlock(br, separator);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String readBlock(BufferedReader br, String separator) throws IOException {
        StringBuilder res = new StringBuilder();
        while (br.ready()) {
            String line = br.readLine();
            res.append(line);
            if (line.endsWith(separator)) {
                return res.toString().replace(separator, "");
            } else {
                res.append("\n");
            }
        }
        return res.toString();
    }

    public long getHashReadingTime() {
        return hashReadingTime;
    }

    public long getOriginalReadingTime() {
        return originalReadingTime;
    }

    public int getErrorCount() {
        return errorCount;
    }
}
