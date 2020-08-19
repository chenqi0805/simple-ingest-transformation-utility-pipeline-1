package com.amazon.ti.plugins.processor;

import com.amazon.ti.model.record.Record;
import com.amazon.ti.model.record.RecordMetadata;
import com.amazon.ti.model.annotations.TransformationInstancePlugin;
import com.amazon.ti.model.configuration.Configuration;
import com.amazon.ti.plugins.PluginType;
import com.amazon.ti.model.processor.Processor;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * The <b>Stateless Trace Processor</b> converts OTEL-based trace data into our service map format. The assumption
 * behind this processor is that the OTEL-based collector has already processed the traces and we simply provide a
 * conversion into our format.
 */
@TransformationInstancePlugin(name="stateless-trace-processor", type = PluginType.PROCESSOR)
public class StatelessServiceMapTraceProcessor implements Processor<Record<String>, Record<String>> {
    //The type of Record we expect to process
    private static final String EXPECTED_DATA_TYPE = "otel-trace";
    //The type of Record we are going to create
    private static final String RESULT_DATA_TYPE = "service-map";

    /**
     * Receives the configuration for the stateless trace processor, and creates the processor with the given settings.
     * @param configuration the configuration settings for the processor.
     */
    public StatelessServiceMapTraceProcessor(final Configuration configuration) {
        //right now we just no-op the configuration
        this();
    }

    /**
     * Initializes the Stateless Trace Processor with some default (hopefully sane) settings.
     */
    public StatelessServiceMapTraceProcessor() {

    }

    private Record<String> convert(Record<String> record) {
        //TODO: write
        return null;
    }

    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        //ensure that the record is of the correct type
        //receive the OTEL trace records
        //convert to JSON
        //build the service map record
        //output the trace record
        return records.stream()
            .filter(i -> EXPECTED_DATA_TYPE.equals(i.getMetadata().getAsString(RecordMetadata.RECORD_TYPE)))
            .map(this::convert).collect(Collectors.toList());
    }
}
