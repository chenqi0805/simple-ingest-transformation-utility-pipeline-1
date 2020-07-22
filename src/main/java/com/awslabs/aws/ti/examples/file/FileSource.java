package com.awslabs.aws.ti.examples.file;

import com.awslabs.aws.ti.Record;
import com.awslabs.aws.ti.buffer.TIBuffer;
import com.awslabs.aws.ti.source.Source;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileSource implements Source {
    private String filePathToRead;
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample.txt";

    public FileSource() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSource(final String filePath) {
        this.filePathToRead = filePath;
    }


    @Override
    public void start(final TIBuffer buffer) {
        try(final BufferedReader reader =
                    Files.newBufferedReader(Paths.get(filePathToRead), StandardCharsets.UTF_8)) {
            String line;
            while((line = reader.readLine()) != null) {
                final byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                buffer.put(new Record(ByteBuffer.wrap(lineBytes)));
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
