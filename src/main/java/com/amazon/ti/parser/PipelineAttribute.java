package com.amazon.ti.parser;

public enum PipelineAttribute {
    PIPELINE("pipeline", true),
    NAME("name", true),
    SOURCE("source", true),
    BUFFER("buffer", false),
    PROCESSOR("processor", false),
    SINK("sink", true);

    private final String name;
    private final boolean isRequired;

    PipelineAttribute(final String name, boolean isRequired) {
        this.name = name;
        this.isRequired = isRequired;
    }

    public String attributeName() {
        return name;
    }

    public boolean isRequired() {
        return isRequired;
    }
}
