package deduplicator;

import deduplicator.db.DbClient;
import deduplicator.hash.HashGeneratorWithTimer;
import deduplicator.service.ReaderService;
import deduplicator.service.WriterService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

import static deduplicator.hash.HashGenerators.*;

public class DeduplicatorApp {
    private static boolean CALCULATE_MISTAKES = false;
    public static int BLOCK_SIZE;
    private static int MAX_POSITION_IN_FILE;
    private static HashGeneratorWithTimer hashGenerator;
    private static DbClient client;
    public static final String SEPARATOR = "#";
    public static final String WAREHOUSE_PATH = "./target/warehouse/";
    public static final String DATA_PATH = "./data.csv";
    public static final String HASHED_PATH = "./target/warehouse/hashed/";
    public static final String ORIGINAL_PATH = "./target/warehouse/original/";
    public static final String REPORT_PATH = "./report";

    public static void main(String[] args) {
        List<Integer> blockSizes = List.of(16, 32, 64);
        List<Integer> maxPositionsInFile = List.of(20);
        List<HashGeneratorWithTimer> generators = List.of(MURMUR.generator);
        for (HashGeneratorWithTimer g : generators) {
            hashGenerator = g;
            for (int bs : blockSizes) {
                BLOCK_SIZE = bs;
                for (int m : maxPositionsInFile) {
                    MAX_POSITION_IN_FILE = m;
                    init();
                    WriterService writer = new WriterService(hashGenerator, HASHED_PATH);
                    writer.startWriting(BLOCK_SIZE, MAX_POSITION_IN_FILE, SEPARATOR, DATA_PATH, client);
                    ReaderService reader = new ReaderService(CALCULATE_MISTAKES);
                    reader.read(HASHED_PATH + hashGenerator.getHashName() + "/", hashGenerator.getHashName(), client, SEPARATOR, WAREHOUSE_PATH, DATA_PATH, BLOCK_SIZE);
                    generateWriterReport(writer.getUniqueValues(), writer.getDuplicates(), writer.getTimeOfHashedDataWriting(), getFolderSize(new File(HASHED_PATH + hashGenerator.getHashName())));
                    generateReaderReport(reader.getErrorCount(),reader.getHashReadingTime(), client.getStats(hashGenerator.getHashName()));
                }
            }
        }
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

    private static void generateWriterReport(int uniqueValues, int duplicates, long timeOfHashedDataWriting, long folderSize) {
        StringBuilder builder = new StringBuilder()
            .append("\nWRITING:")
            .append("\nBlock's size: ").append(BLOCK_SIZE)
            .append("\nMax position in file: ").append(MAX_POSITION_IN_FILE)
            .append("\nUnique values: ").append(uniqueValues)
            .append("\nDuplicates: ").append(duplicates)
            .append("\nFolder size: ").append(folderSize)
            .append("\nTime of ").append(hashGenerator.getHashName()).append(": ").append(hashGenerator.getTime() + timeOfHashedDataWriting);
        try (PrintWriter bw = new PrintWriter(new BufferedWriter(new FileWriter(REPORT_PATH, true)))) {
            bw.println(builder.toString());
            System.out.println(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static void generateReaderReport(int errorCount, long timeOfHashedDataReading, int storageSize) {
        StringBuilder builder = new StringBuilder()
            .append("\nREADING:")
            .append("\nError count: ").append(errorCount)
            .append("\nTime of hashed data reading: ").append(timeOfHashedDataReading)
            .append("\nBD storage size: ").append(storageSize);

        try (PrintWriter bw = new PrintWriter(new BufferedWriter(new FileWriter(REPORT_PATH, true)))) {
            bw.println(builder.toString());
            System.out.println(builder.toString());
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private static long getFolderSize(File folder) {
        long length = 0;
        File[] files = folder.listFiles();
        int count = files.length;
        for (int i = 0; i < count; i++) {
            if (files[i].isFile()) {
                length += files[i].length();
            } else {
                length += getFolderSize(files[i]);
            }
        }
        return length;
    }
}