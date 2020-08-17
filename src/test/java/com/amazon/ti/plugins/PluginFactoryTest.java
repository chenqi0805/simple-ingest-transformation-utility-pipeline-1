package com.amazon.ti.plugins;

import com.amazon.ti.configuration.Configuration;
import com.amazon.ti.source.Source;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class PluginFactoryTest {
    public static final Configuration NON_EXISTENT_EMPTY_CONFIGURATION = new Configuration("does-not-exists", new HashMap<>());

    @Test
    public void testNoMandatoryConstructor() {
        final Configuration testConfiguration = new Configuration("junit-test", new HashMap<>());
        final Class<Source> clazz = PluginRepository.getSourceClass(testConfiguration.getName());
        assertNotNull(clazz);
        //assertThrows(PluginException.class, ()->PluginFactory.newPlugin(testConfiguration, clazz));
        try{
            PluginFactory.newPlugin(testConfiguration, clazz);
        } catch (PluginException e) {
            assertTrue("Incorrect exception or exception message was thrown", e.getMessage().startsWith(
                    "TransformationInstance plugin requires a constructor with Configuration parameter; Plugin " +
                            "ConstructorLessComponent with name junit-test is missing such constructor."));
        }
    }

}
