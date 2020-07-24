package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

public class FileSink implements Sink<Record<String>> {
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    private final String outputFilePath;

    public FileSink() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSink(final String outputFile) {
        this.outputFilePath = outputFile;
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        try(final BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for(final Record<String> record : records) {
                writer.write(record.getData());
                writer.newLine();
            }
            return true;
        } catch (IOException ex) {
            System.err.println(ex.getMessage());
            return false;
        }
    }

    @Override
    public void stop() {
        //No Op
    }
}
