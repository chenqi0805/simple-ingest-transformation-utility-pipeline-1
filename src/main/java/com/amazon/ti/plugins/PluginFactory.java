package com.amazon.ti.plugins;

import com.amazon.ti.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static java.lang.String.format;

public class PluginFactory {

    public static Object newPlugin(final Configuration configuration, final Class<?> clazz) {
        try {
            final Constructor<?> constructor = clazz.getConstructor(Configuration.class);
            return constructor.newInstance(configuration);
        } catch (NoSuchMethodException e) {
            throw new PluginException(format("TransformationInstance plugin requires a constructor with %s parameter;" +
                            " Plugin %s is missing such constructor ", Configuration.class.getSimpleName(),
                    clazz.getSimpleName()));
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new PluginException(format("Encountered %s exception while instantiating the plugin %s",
                    e.getMessage(), clazz.getSimpleName()));
        }
    }
}
