package com.amazon.ti.plugins;

import com.amazon.ti.model.configuration.PluginSetting;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static java.lang.String.format;

public class PluginFactory {

    public static Object newPlugin(final PluginSetting pluginSetting, final Class<?> clazz) {
        if (clazz == null) {
            throw new PluginException(format("Failed to find the plugin with name [%s]. " +
                    "Please ensure that plugin is annotated with appropriate values", pluginSetting.getName()));
        }
        try {
            final Constructor<?> constructor = clazz.getConstructor(PluginSetting.class);
            return constructor.newInstance(pluginSetting);
        } catch (NoSuchMethodException e) {
            throw new PluginException(format("TransformationInstance plugin requires a constructor with %s parameter;" +
                            " Plugin %s with name %s is missing such constructor.", PluginSetting.class.getSimpleName(),
                    clazz.getSimpleName(), pluginSetting.getName()));
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new PluginException(format("Encountered %s exception while instantiating the plugin %s",
                    e.getMessage(), clazz.getSimpleName()));
        }
    }
}
