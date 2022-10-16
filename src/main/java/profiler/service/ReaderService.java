package profiler.service;

import profiler.db.DbClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ReaderService {
    private long hashReadingTime = 0;
    private long originalReadingTime = 0;
    private int errorCount = 0;


    public void read(String hashedPath, String hashName, DbClient client, String originalPath, String separator, String warehousePath) {
        File dir = new File(hashedPath);
        StringBuilder dataFromHash = new StringBuilder();
        for (File file : dir.listFiles()) {
            String fileName = file.getName();
            long t1 = System.nanoTime();
            try (BufferedReader br = new BufferedReader(new FileReader(dir + "/" + fileName, StandardCharsets.UTF_8))) {
                while (br.ready()) {
                    String hash = readBlock(br, separator);
                    if (client.isElementPresent(hash, hashName)) {
                        int[] filePos = client.findByHash(hash, hashName);
                        int f = filePos[0];
                        int pos = filePos[1];

                        dataFromHash.append(readHashedDataFromFile(f, hash, pos, separator));
                    } else {
                        dataFromHash.append(hash);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            hashReadingTime += System.nanoTime() - t1;
            t1 = System.nanoTime();
            readOriginalDataFromFile(file.getName(), originalPath);
            originalReadingTime += System.nanoTime() - t1;
        }
        writeToFile(warehousePath, dataFromHash.toString());
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
}
