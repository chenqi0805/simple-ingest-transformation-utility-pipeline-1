package com.amazon.ti.plugins.source;

import com.amazon.ti.Record;
import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.source.Source;

import java.util.Scanner;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple source which reads data from console each line at a time. It exits when it reads case insensitive "exit"
 * from console or if {@link com.amazon.ti.pipeline.Pipeline} notifies to stop.
 */
@TransformationInstancePlugin(name = "stdin", type = PluginType.SOURCE)
public class StdInSource implements Source<Record<String>> {
    private final Scanner reader;
    private boolean isStopRequested;

    /**
     * Mandatory constructor for Transformation Instance Component - This constructor is used by Transformation instance
     * runtime engine to construct an instance of {@link StdInSource} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StdInSource(final PluginSetting pluginSetting) {
        this();
    }

    public StdInSource() {
        reader = new Scanner(System.in);
        isStopRequested = false;
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        checkNotNull(buffer, "buffer cannot be null for source to start");
        String line = reader.nextLine();
        while (!"exit".equalsIgnoreCase(line) && !isStopRequested) {
            final Record<String> record = new Record<>(line);
            buffer.write(record);
            line = reader.nextLine();
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }
}
