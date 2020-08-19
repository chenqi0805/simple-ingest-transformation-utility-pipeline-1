package com.amazon.ti.plugins.buffer;

import com.amazon.ti.model.buffer.Buffer;
import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginException;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.ti.plugins.PluginFactoryTest.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class BufferFactoryTest {

    /**
     * Tests if BufferFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testNewBufferClassByNameThatExists() {
        final PluginSetting unboundedBufferConfiguration = new PluginSetting("unbounded-inmemory", new HashMap<>());
        final Buffer actualBuffer = BufferFactory.newBuffer(unboundedBufferConfiguration);
        final Buffer expectedBuffer = new UnboundedInMemoryBuffer();
        assertNotNull(actualBuffer);
        assertEquals(expectedBuffer.getClass().getSimpleName(), actualBuffer.getClass().getSimpleName());
    }

    /**
     * Tests if BufferFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSinkRetrieval() {
        assertThrows(PluginException.class, () -> BufferFactory.newBuffer(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
