package profiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.db.DbClient;
import profiler.hash.*;
import profiler.service.ProfilerService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ProfilerWriterApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProfilerWriterApp.class);
    public static final String WAREHOUSE_PATH = "./target/warehouse/";
    public static final String HASHED_PATH = "./target/warehouse/hashed/";
    public static final String ORIGINAL_PATH = "./target/warehouse/original/";
    public static final int BLOCKS_COUNT = 10000;
    private static DbClient client;
    private static List<HashGeneratorWithTimer> hashGenerators;

    public static void main(String[] args) throws IOException {
        init();
        for (HashGeneratorWithTimer generator : hashGenerators){
            client.clearCollection(generator.getHashName());
        }
        ProfilerService service = new ProfilerService();
        long timeOfOriginalDataWriting = 0;
        Set<String> uniqueValues = new HashSet<>();

        for (int i = 0; i < BLOCKS_COUNT; i++) {
            String virtualMemoryData = service.getMemory();
            for (HashGeneratorWithTimer generator : hashGenerators) {
                long t1 = System.nanoTime();
                String hash = generator.getHash(virtualMemoryData).replaceAll("[\\r\\n]+", "");
                client.writeHash(hash, virtualMemoryData, generator.getHashName());
                writeOsDataToFile(hash, String.valueOf(i), HASHED_PATH + generator.getHashName() + "/");
                generator.addTime(System.nanoTime() - t1);
            }
            long t1 = System.nanoTime();
            writeOsDataToFile(virtualMemoryData, String.valueOf(i), ORIGINAL_PATH);
            timeOfOriginalDataWriting += System.nanoTime() - t1;
            uniqueValues.add(virtualMemoryData);
        }
        generateReport(uniqueValues.size(), timeOfOriginalDataWriting);
    }

    private static void init() {
        hashGenerators = new ArrayList<>();
        hashGenerators.add(new Md5HashGenerator());
        hashGenerators.add(new MurMurHashGenerator());
        hashGenerators.add(new Sha512HashGenerator());

        try {
            client = new DbClient();
        } catch (Exception e) {
            System.out.println("Can't create db client: " + e.getMessage());
            e.printStackTrace();
        }
        File dir = new File(WAREHOUSE_PATH);
        if (dir.exists()) {
            Arrays.stream(dir.listFiles()).forEach(File::delete);
            dir.delete();
        }
        dir.mkdir();
        new File(ORIGINAL_PATH).mkdir();
        new File(HASHED_PATH).mkdir();
        for (HashGenerator generator : hashGenerators) {
            new File(HASHED_PATH + generator.getHashName()).mkdir();
        }
    }

    private static void writeOsDataToFile(String data, String fileName, String dir) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dir + fileName, StandardCharsets.UTF_8))) {
            bw.write(data);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void generateReport(int duplicates, long timeOfOriginalDataWriting) {
        StringBuilder builder = new StringBuilder()
            .append("\nWRITING:")
            .append("\nBlock's count: ").append(BLOCKS_COUNT)
            .append("\nUnique values: ").append(duplicates / hashGenerators.size())
            .append("\nTime of original data writing: ").append(timeOfOriginalDataWriting);
        for (HashGeneratorWithTimer generator : hashGenerators) {
            builder.append("\n Time of ").append(generator.getHashName()).append(":").append(generator.getTime());
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(WAREHOUSE_PATH + "report"))) {
            bw.write(builder.toString());
            LOGGER.info(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}