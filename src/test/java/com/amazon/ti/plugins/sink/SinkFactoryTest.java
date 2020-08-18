package com.amazon.ti.plugins.sink;

import com.amazon.ti.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginException;
import com.amazon.ti.sink.Sink;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.ti.plugins.PluginFactoryTest.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class SinkFactoryTest {

    /**
     * Tests if SinkFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewSinkClassByNameThatExists() {
        final PluginSetting stdOutSinkConfiguration = new PluginSetting("stdout", new HashMap<>());
        final Sink actualSink = SinkFactory.newSink(stdOutSinkConfiguration);
        final Sink expectedSink = new StdOutSink();
        assertNotNull(actualSink);
        assertEquals(expectedSink.getClass().getSimpleName(), actualSink.getClass().getSimpleName());
    }

    /**
     * Tests if SinkFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> SinkFactory.newSink(NON_EXISTENT_EMPTY_CONFIGURATION));
    }

}
