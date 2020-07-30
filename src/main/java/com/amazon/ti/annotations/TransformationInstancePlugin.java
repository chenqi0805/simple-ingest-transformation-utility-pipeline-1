package com.amazon.ti.annotations;

import com.amazon.ti.plugins.PluginType;

import java.lang.annotation.*;

/**
 * Annotates a Transformation Instance Java plugin that includes Source, Sink, Buffer and Processor.
 * The value returned from {@link #name()} represents the name of the plugin and is used in the pipeline configuration
 * and the optional {@link #type()}
 *
 * TODO 1. Pick a different name - Plugin, Component, Resource conflicts with
 * other most used frameworks and may confuse users
 * TODO 2. Add capability for ElementType.METHOD
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface TransformationInstancePlugin {
    /**
     *
     * @return Name of the plugin which should be unique for the type
     */
    String name();

    PluginType type();
}
