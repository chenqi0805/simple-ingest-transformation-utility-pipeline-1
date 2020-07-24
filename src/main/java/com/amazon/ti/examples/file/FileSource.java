package com.amazon.ti.examples.file;

import com.amazon.ti.Record;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.source.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileSource implements Source<Record<String>> {
    private final String filePathToRead;
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample.txt";

    public FileSource() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSource(final String filePath) {
        this.filePathToRead = filePath;
    }


    @Override
    public void start(final Buffer<Record<String>> buffer) {
        try (final BufferedReader reader =
                     Files.newBufferedReader(Paths.get(filePathToRead), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                buffer.put(new Record<>(line));
            }
        } catch (IOException ex) {
            //exception processing the File
            System.err.format("IOException: %s%n", ex);
        }
    }

    @Override
    public void stop() {
        //no op
    }
}
