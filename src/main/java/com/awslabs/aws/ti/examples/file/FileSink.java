package com.awslabs.aws.ti.examples.file;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

public class FileSink implements Sink {
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    private String outputFilePath;

    public FileSink() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSink(final String outputFile) {
        this.outputFilePath = outputFile;
    }

    @Override
    public boolean output(Collection<Record> records) {
        try(final BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for(final Record record : records) {
                writer.write(new String(record.getData().array(), StandardCharsets.UTF_8));
                writer.newLine();
            }
            return true;
        } catch (IOException ex) {
            System.err.println(ex);
            return false;
        }
    }

    @Override
    public void stop() {
        //No Op
    }
}
