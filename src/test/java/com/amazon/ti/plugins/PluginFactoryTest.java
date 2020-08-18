package com.amazon.ti.plugins;

import com.amazon.ti.configuration.PluginSetting;
import com.amazon.ti.source.Source;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
public class PluginFactoryTest {
    public static final PluginSetting NON_EXISTENT_EMPTY_CONFIGURATION = new PluginSetting("does-not-exists", new HashMap<>());

    @Test
    public void testNoMandatoryConstructor() {
        final PluginSetting testPluginSettings = new PluginSetting("junit-test", new HashMap<>());
        final Class<Source> clazz = PluginRepository.getSourceClass(testPluginSettings.getName());
        assertNotNull(clazz);
        //assertThrows(PluginException.class, ()->PluginFactory.newPlugin(testPluginSettings, clazz));
        try {
            PluginFactory.newPlugin(testPluginSettings, clazz);
        } catch (PluginException e) {
            assertTrue("Incorrect exception or exception message was thrown", e.getMessage().startsWith(
                    "TransformationInstance plugin requires a constructor with PluginSetting parameter; Plugin " +
                            "ConstructorLessComponent with name junit-test is missing such constructor."));
        }
    }

}
