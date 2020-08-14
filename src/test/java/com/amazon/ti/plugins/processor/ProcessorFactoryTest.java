package com.amazon.ti.plugins.processor;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.plugins.PluginException;
import com.amazon.ti.plugins.sink.SinkFactory;
import com.amazon.ti.plugins.sink.StdOutSink;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.ti.plugins.PluginFactoryTest.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class ProcessorFactoryTest {

    /**
     * Tests if ProcessorFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final Configuration noOpProcessorConfiguration = new Configuration("no-op", new HashMap<>());
        final Processor actualProcessor = ProcessorFactory.newProcessor(noOpProcessorConfiguration);
        final Processor expectedProcessor = new NoOpProcessor();
        assertNotNull(actualProcessor);
        assertEquals(expectedProcessor.getClass().getSimpleName(), actualProcessor.getClass().getSimpleName());
    }
    /**
     * Tests if ProcessorFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
