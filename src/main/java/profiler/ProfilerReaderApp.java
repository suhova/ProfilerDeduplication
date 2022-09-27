package profiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.db.DbClient;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

import static profiler.ProfilerWriterApp.*;

public class ProfilerReaderApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProfilerReaderApp.class);

    public static void main(String[] args) {
        DbClient client = new DbClient();
        String alg = "md5";
        int errorCnt = 0;
        long originalReadingTime = 0;
        long hashReadingTime = 0;
        for (int i = 0; i < BLOCKS_COUNT; i++) {
            long t1 = System.nanoTime();
            String hash = readOsDataFromFile(String.valueOf(i), HASHED_PATH + alg + "/");
            String data = client.findByHash(hash, alg);
            hashReadingTime += System.nanoTime() - t1;
            t1 = System.nanoTime();
            String expectedData = readOsDataFromFile(String.valueOf(i), ORIGINAL_PATH);
            originalReadingTime = System.nanoTime() - t1;

            if (!data.replaceAll("[\\r\\n]+", "").equals(expectedData)) {
                errorCnt++;
            }
        }

        LOGGER.info("errorCnt: " + errorCnt);
        LOGGER.info("hashReadingTime: " + hashReadingTime);
        LOGGER.info("originalReadingTime: " + originalReadingTime);
    }

    private static String readOsDataFromFile(String fileName, String dir) {
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
}
