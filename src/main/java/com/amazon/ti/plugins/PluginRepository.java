package com.amazon.ti.plugins;

import com.amazon.ti.annotations.TransformationInstancePlugin;
import com.amazon.ti.buffer.Buffer;
import com.amazon.ti.processor.Processor;
import com.amazon.ti.sink.Sink;
import com.amazon.ti.source.Source;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Plugin repository for Transformation Instance - provides methods to discover new plugins
 * and also the default provided plugins.
 * <p>
 * TODO Add capability to discover custom new plugins
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class PluginRepository {
    private static final String DEFAULT_PLUGINS_CLASSPATH = "com.amazon.ti.plugins";
    private static final Map<String, Class<Source>> SOURCES = new HashMap<>();
    private static final Map<String, Class<Buffer>> BUFFERS = new HashMap<>();
    private static final Map<String, Class<Processor>> PROCESSORS = new HashMap<>();
    private static final Map<String, Class<Sink>> SINKS = new HashMap<>();

    static {
        findPlugins();
    }

    private static void findPlugins() {
        final Reflections reflections = new Reflections(DEFAULT_PLUGINS_CLASSPATH);
        final Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(TransformationInstancePlugin.class);
        for (final Class<?> annotatedClass : annotatedClasses) {
            final TransformationInstancePlugin tiPluginAnnotation = annotatedClass
                    .getAnnotation(TransformationInstancePlugin.class);
            final String pluginName = tiPluginAnnotation.name();
            final PluginType pluginType = tiPluginAnnotation.type();
            switch(pluginType) {
                case SOURCE:
                    SOURCES.put(pluginName, (Class<Source>) annotatedClass);
                    break;
                case BUFFER:
                    BUFFERS.put(pluginName, (Class<Buffer>) annotatedClass);
                    break;
                case PROCESSOR:
                    PROCESSORS.put(pluginName, (Class<Processor>) annotatedClass);
                    break;
                case SINK:
                    SINKS.put(pluginName, (Class<Sink>) annotatedClass);
                    break;
            }
        }
    }

    public static Class<?> getPluginClass(final String name, final PluginType pluginType) {
        switch (pluginType) {
            case SOURCE:
                return getSourceClass(name);
            case BUFFER:
                return getBufferClass(name);
            case PROCESSOR:
                return getProcessorClass(name);
            case SINK:
                return getSinkClass(name);
        }
        throw new PluginException("Unrecognized plugin type: " + pluginType);
    }

    public static Class<Source> getSourceClass(final String name) {
        return SOURCES.get(name);
    }

    public static Class<Buffer> getBufferClass(final String name) {
        return BUFFERS.get(name);
    }

    public static Class<Processor> getProcessorClass(final String name) {
        return PROCESSORS.get(name);
    }

    public static Class<Sink> getSinkClass(final String name) {
        return SINKS.get(name);
    }

}
