package com.amazon.ti.plugins.sink;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.sink.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

@TransformationInstancePlugin(name = "file", type = PluginType.SINK)
public class FileSink implements Sink<Record<String>> {
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample-output.txt";

    private final String outputFilePath;

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link FileSink} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     *
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public FileSink(final Configuration configuration) {
        this((String) configuration.getAttributeFromMetadata("path"));
    }

    public FileSink() {
        this(SAMPLE_FILE_PATH);
    }

    public FileSink(final String outputFile) {
        this.outputFilePath = outputFile;
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        try (final BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath),
                StandardCharsets.UTF_8)) {
            for (final Record<String> record : records) {
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
