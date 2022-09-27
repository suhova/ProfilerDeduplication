package profiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.db.DbClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static profiler.ProfilerWriterApp.*;

public class ProfilerReaderApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProfilerReaderApp.class);

    public static void main(String[] args) {
        DbClient client = new DbClient();
        String alg = "sha512";
        int errorCnt = 0;
        long t1 = System.nanoTime();
        for (int i = 0; i < BLOCKS_COUNT; i++) {
            try{
                String hash = readStringFromFile(String.valueOf(i),HASHED_PATH + alg + "/");
                LOGGER.info("hash: " + hash);
                LOGGER.debug(client.findByHash(hash, alg));
            } catch (NullPointerException e){
                LOGGER.error("! " + i);
                errorCnt++;
            }
        }
        long hashReadingTime = System.nanoTime() - t1;

        t1 = System.nanoTime();
//        for (int i = 0; i < BLOCKS_COUNT; i++) {
//            LOGGER.debug(readOsDataFromFile(String.valueOf(i)));
//        }
        long originalReadingTime = System.nanoTime() - t1;

        LOGGER.info("errorCnt: "  + errorCnt);
        LOGGER.info("hashReadingTime: "  + hashReadingTime);
        LOGGER.info("originalReadingTime: "  + originalReadingTime);
    }

    private static String readStringFromFile(String fileName, String dir) {
        try (BufferedReader br = new BufferedReader(new FileReader(dir + fileName))) {
            return br.readLine();
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }
    private static String readOsDataFromFile(String fileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(ORIGINAL_PATH + fileName, StandardCharsets.UTF_8))) {
            StringBuilder res = new StringBuilder();
            while (br.ready()){
                res.append(br.readLine()).append("\n");
            }
            return res.toString();
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
