package deduplicator;

import deduplicator.db.DbClient;
import deduplicator.hash.HashGeneratorWithTimer;
import deduplicator.service.ReaderService;
import deduplicator.service.WriterService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static deduplicator.hash.HashGenerators.MD5;

public class DeduplicatorApp {
    public static final int BLOCK_SIZE = 400;
    private static final int MAX_POSITION_IN_FILE = 1280;
    private static final boolean CALCULATE_MISTAKES = true;
    private static DbClient client;
    private static HashGeneratorWithTimer hashGenerator = MD5.generator;
    public static final String SEPARATOR = "#";
    public static final String WAREHOUSE_PATH = "./target/warehouse/";
    public static final String DATA_PATH = "./datamini.csv";
    public static final String HASHED_PATH = "./target/warehouse/hashed/";
    public static final String ORIGINAL_PATH = "./target/warehouse/original/";

    public static void main(String[] args) {
        init();

        WriterService writer = new WriterService(hashGenerator, HASHED_PATH, ORIGINAL_PATH);
        writer.startWriting(BLOCK_SIZE, MAX_POSITION_IN_FILE, SEPARATOR, DATA_PATH, client);
        ReaderService reader = new ReaderService(CALCULATE_MISTAKES);
        reader.read(HASHED_PATH + hashGenerator.getHashName() + "/", hashGenerator.getHashName(), client, ORIGINAL_PATH, SEPARATOR, WAREHOUSE_PATH, DATA_PATH, BLOCK_SIZE);
        generateWriterReport(writer.getUniqueValues(), writer.getTimeOfOriginalDataWriting(), writer.getTimeOfHashedDataWriting());
        generateReaderReport(reader.getErrorCount(), reader.getOriginalReadingTime(), reader.getHashReadingTime());
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

    private static void generateWriterReport(int duplicates, long timeOfOriginalDataWriting, long timeOfHashedDataWriting) {
        StringBuilder builder = new StringBuilder()
            .append("\nWRITING:")
            .append("\nBlock's size: ").append(BLOCK_SIZE)
            .append("\nMax position in file: ").append(MAX_POSITION_IN_FILE)
            .append("\nUnique values: ").append(duplicates)
            .append("\nTime of original data writing: ").append(timeOfOriginalDataWriting)
            .append("\nTime of ").append(hashGenerator.getHashName()).append(":").append(hashGenerator.getTime() + timeOfHashedDataWriting);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(WAREHOUSE_PATH + "report"))) {
            bw.write(builder.toString());
            System.out.println(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
    private static void generateReaderReport(int errorCount, long timeOfOriginalDataReading, long timeOfHashedDataReading) {
        StringBuilder builder = new StringBuilder()
            .append("\nREADING:")
            .append("\nError count: ").append(errorCount)
            .append("\nTime of original data reading: ").append(timeOfOriginalDataReading)
            .append("\nTime of hashed data reading:").append(timeOfHashedDataReading);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(WAREHOUSE_PATH + "reportRead"))) {
            bw.write(builder.toString());
            System.out.println(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }
}