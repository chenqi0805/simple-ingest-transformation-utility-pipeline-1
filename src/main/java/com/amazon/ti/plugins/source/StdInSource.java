package com.amazon.ti.plugins.source;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.source.Source;

import java.util.Scanner;

/**
 * Sample source for standard input
 */
@TransformationInstancePlugin(name = "stdin", type = PluginType.SOURCE)
public class StdInSource implements Source<Record<String>> {
    private final Scanner reader;
    private boolean haltFlag;

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link StdInSource} using an instance of {@link Configuration} which
     * has access to configuration metadata from pipeline
     * configuration file.
     *
     * @param configuration instance with metadata information from pipeline configuration file.
     */
    public StdInSource(final Configuration configuration) {
        this();
    }

    public StdInSource() {
        reader = new Scanner(System.in);
        haltFlag = false;
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        if (buffer == null) {
            //exception scenario
            return;
        }
        String line = "";
        while (!haltFlag && !"exit".equalsIgnoreCase(line)) {
            line = reader.nextLine();
            final Record<String> record = new Record<>(line);
            buffer.write(record);
        }
    }

    @Override
    public void stop() {
        haltFlag = true;
    }
}
