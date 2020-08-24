package com.amazon.ti.plugins.source;

import com.amazon.ti.model.configuration.PluginSetting;
import com.amazon.ti.plugins.PluginException;
import com.amazon.ti.model.source.Source;
import org.junit.Test;

import java.util.HashMap;

import static com.amazon.ti.plugins.PluginFactoryTest.NON_EXISTENT_EMPTY_CONFIGURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@SuppressWarnings("rawtypes")
public class SourceFactoryTest {

    /**
     * Tests if SourceFactory is able to retrieve default Source plugins by name
     */
    @Test
    public void testSourceClassByName() {
        final PluginSetting stdInSourceConfiguration = new PluginSetting("stdin", new HashMap<>());
        final Source actualSource = SourceFactory.newSource(stdInSourceConfiguration);
        final Source expectedSource = new StdInSource();
        assertNotNull(actualSource);
        assertEquals(expectedSource.getClass().getSimpleName(), actualSource.getClass().getSimpleName());
    }

    /**
     * Tests if SourceFactory fails with correct Exception when queried for a non-existent plugin
     */
    @Test
    public void testNonExistentSourceRetrieval() {
        assertThrows(PluginException.class, () -> SourceFactory.newSource(NON_EXISTENT_EMPTY_CONFIGURATION));
    }
}
