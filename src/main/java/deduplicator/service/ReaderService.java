package deduplicator.service;

import deduplicator.db.DbClient;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ReaderService {
    private long hashReadingTime = 0;
    private int errorCount = 0;
    private final boolean calculateMistakes;

    public ReaderService(boolean calculateMistakes) {
        this.calculateMistakes = calculateMistakes;
    }

    public void read(String hashedPath, String hashName, DbClient client, String separator,
                     String warehousePath, String sourceFilePath, int blockSize) {
        File dir = new File(hashedPath);
        StringBuilder dataFromHash = new StringBuilder();
        List<File> files = Arrays.stream(dir.listFiles()).sorted(Comparator.comparingInt(a -> Integer.parseInt(a.getName()))).collect(Collectors.toList());

        try (FileInputStream is = new FileInputStream(sourceFilePath)) {
            for (File file : files) {
                String fileName = file.getName();
                long t1 = System.nanoTime();
                try (
                    BufferedReader br = new BufferedReader(new InputStreamReader(new BlockLZ4CompressorInputStream(new FileInputStream(dir + "/" + fileName))));
                ) {
                    if (calculateMistakes) {
                        byte[] chunk = new byte[blockSize];
                        String line;
                        while ((line = br.readLine()) != null) {
                            String expected = "";
                            if (is.read(chunk) != -1) {
                                expected = new String(chunk).replace("\0", "");
                            }
                            String actual = getDataFromHash(br, separator, client, hashName, line, dir.getAbsolutePath());
                            dataFromHash.append(actual);
                            if (!actual.equals(expected)) {
                                errorCount++;
                                System.out.println("ErrorCnt=" + errorCount + "\n Expected block: " + expected + ", actual: " + actual);
                            }
                            chunk = new byte[blockSize];
                        }
                    } else {
                        String line;
                        while ((line = br.readLine()) != null) {
                            dataFromHash.append(getDataFromHash(br, separator, client, hashName, line, dir.getAbsolutePath()));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                hashReadingTime += System.nanoTime() - t1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeToFile(warehousePath, dataFromHash.toString());
    }

    private String getDataFromHash(BufferedReader br, String separator, DbClient client, String hashName, String firstLine, String dir) throws IOException {
        String hash = readBlock(br, separator, firstLine);
        if (client.isElementPresent(hash, hashName)) {
            int[] filePos = client.findByHash(hash, hashName);
            int f = filePos[0];
            int pos = filePos[1];
            return readHashedDataFromFile(f, dir, pos, separator);
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

    private String readHashedDataFromFile(int fileName, String dir, int position, String separator) {
         try(BufferedReader br = new BufferedReader(new InputStreamReader(new BlockLZ4CompressorInputStream(new FileInputStream(dir + "/" + fileName))))){
            int curPos = 0;
            String line = br.readLine();
            while (curPos < position) {
                readBlock(br, separator, line);
                line = br.readLine();
                curPos++;
            }
            return readBlock(br, separator, line);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String readBlock(BufferedReader br, String separator, String firstLine) throws IOException {
        if (firstLine.endsWith(separator)) {
            return firstLine.replace(separator, "");
        }
        StringBuilder res = new StringBuilder().append(firstLine).append("\n");
        String line;
        while ((line = br.readLine()) != null) {
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

    public int getErrorCount() {
        return errorCount;
    }
}
