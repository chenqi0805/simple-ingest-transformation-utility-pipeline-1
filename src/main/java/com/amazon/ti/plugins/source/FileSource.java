package com.amazon.ti.plugins.source;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.source.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@TransformationInstancePlugin(name = "file", type = PluginType.SOURCE)
public class FileSource implements Source<Record<String>> {
    private final String filePathToRead;
    private static final String SAMPLE_FILE_PATH = "src/resources/file-test-sample.txt";

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link FileSource} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public FileSource(final Configuration configuration) {
        this((String) configuration.getAttributeFromMetadata("path"));
    }

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
                buffer.write(new Record<>(line));
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
