package profiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.db.DbClient;
import profiler.hash.HashGeneratorWithTimer;
import profiler.hash.HashGenerators;
import profiler.service.ReaderService;
import profiler.service.WriterService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static profiler.hash.HashGenerators.MD5;

public class DeduplicatorApp {
    public static final int BLOCK_SIZE = 400;
    private static final int MAX_POSITION_IN_FILE = 1280;
    private static DbClient client;
    private static HashGeneratorWithTimer hashGenerator = MD5.generator;
    public static final String SEPARATOR = "#";
    private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicatorApp.class);
    public static final String WAREHOUSE_PATH = "./target/warehouse/";
    public static final String DATA_PATH = "./datamini.csv";
    public static final String HASHED_PATH = "./target/warehouse/hashed/";
    public static final String ORIGINAL_PATH = "./target/warehouse/original/";

    public static void main(String[] args) {
        init();

        WriterService writer = new WriterService(hashGenerator, HASHED_PATH, ORIGINAL_PATH);
        writer.startWriting(BLOCK_SIZE, MAX_POSITION_IN_FILE, SEPARATOR, DATA_PATH, client);
        generateReport(writer.getUniqueValues(), writer.getTimeOfOriginalDataWriting(), writer.getTimeOfHashedDataWriting());
        ReaderService reader = new ReaderService();
        reader.read(HASHED_PATH + hashGenerator.getHashName() + "/", hashGenerator.getHashName(), client, ORIGINAL_PATH, SEPARATOR, WAREHOUSE_PATH);
    }

    private static void init() {
        try {
            client = new DbClient();
        } catch (Exception e) {
            System.out.println("Can't create db client: " + e.getMessage());
            e.printStackTrace();
        }
        client.clearCollection(hashGenerator.getHashName());
        File dir = new File(WAREHOUSE_PATH);
        File original = new File(ORIGINAL_PATH);
        File hashed = new File(HASHED_PATH);
        delete(dir);
        dir.mkdir();
        original.mkdir();
        hashed.mkdir();
        new File(HASHED_PATH + hashGenerator.getHashName()).mkdir();
    }

    private static void delete(File dir) {
        if (dir.exists()) {
            if (dir.isDirectory()) {
                Arrays.stream(dir.listFiles()).forEach(DeduplicatorApp::delete);
            }
            dir.delete();
        }
    }



    private static void generateReport(int duplicates, long timeOfOriginalDataWriting, long timeOfHashedDataWriting) {
        StringBuilder builder = new StringBuilder()
            .append("\nWRITING:")
            .append("\nBlock's size: ").append(BLOCK_SIZE)
            .append("\nMax position in file: ").append(MAX_POSITION_IN_FILE)
            .append("\nUnique values: ").append(duplicates)
            .append("\nTime of original data writing: ").append(timeOfOriginalDataWriting)
            .append("\nTime of ").append(hashGenerator.getHashName()).append(":").append(hashGenerator.getTime() + timeOfHashedDataWriting);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(WAREHOUSE_PATH + "report"))) {
            bw.write(builder.toString());
            LOGGER.info(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}