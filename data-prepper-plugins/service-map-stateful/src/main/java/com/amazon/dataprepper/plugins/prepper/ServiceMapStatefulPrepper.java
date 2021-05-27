package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.prepper.state.MapDbPrepperState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.SignedBytes;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SingleThread
@DataPrepperPlugin(name = "service_map_stateful", type = PluginType.PREPPER)
public class ServiceMapStatefulPrepper extends AbstractPrepper<Record<ExportTraceServiceRequest>, Record<String>> {

    public static final String SPANS_DB_SIZE = "spansDbSize";
    public static final String TRACE_GROUP_DB_SIZE = "traceGroupDbSize";

    private static final Logger LOG = LoggerFactory.getLogger(ServiceMapStatefulPrepper.class);
    private static final String EMPTY_SUFFIX = "-empty";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Collection<Record<String>> EMPTY_COLLECTION = Collections.emptySet();
    private static final Integer TO_MILLIS = 1_000;

    // TODO: This should not be tracked in this class, move it up to the creator
    private static final AtomicInteger preppersCreated = new AtomicInteger(0);
    private static long previousTimestamp;
    private static long windowDurationMillis;
    private static CountDownLatch edgeEvaluationLatch;
    private static CountDownLatch windowRotationLatch = new CountDownLatch(1);
    private volatile static MapDbPrepperState<ServiceMapStateData> previousWindow;
    private volatile static MapDbPrepperState<ServiceMapStateData> currentWindow;
    private volatile static MapDbPrepperState<String> previousTraceGroupWindow;
    private volatile static MapDbPrepperState<String> currentTraceGroupWindow;
    //TODO: Consider keeping this state in a db
    private volatile static HashSet<ServiceMapRelationship> relationshipState = new HashSet<>();
    private static File dbPath;
    private static Clock clock;
    private static int processWorkers;

    private final int thisPrepperId;

    public ServiceMapStatefulPrepper(final PluginSetting pluginSetting) {
        this(pluginSetting.getIntegerOrDefault(ServiceMapPrepperConfig.WINDOW_DURATION, ServiceMapPrepperConfig.DEFAULT_WINDOW_DURATION) * TO_MILLIS,
                new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH),
                Clock.systemUTC(),
                pluginSetting.getNumberOfProcessWorkers(),
                pluginSetting);
    }

    public ServiceMapStatefulPrepper(final long windowDurationMillis,
                                     final File databasePath,
                                     final Clock clock,
                                     final int processWorkers,
                                     final PluginSetting pluginSetting) {
        super(pluginSetting);

        ServiceMapStatefulPrepper.clock = clock;
        this.thisPrepperId = preppersCreated.getAndIncrement();
        if (isMasterInstance()) {
            previousTimestamp = ServiceMapStatefulPrepper.clock.millis();
            ServiceMapStatefulPrepper.windowDurationMillis = windowDurationMillis;
            ServiceMapStatefulPrepper.dbPath = createPath(databasePath);
            ServiceMapStatefulPrepper.processWorkers = processWorkers;
            currentWindow = new MapDbPrepperState<>(dbPath, getNewDbName(), processWorkers);
            previousWindow = new MapDbPrepperState<>(dbPath, getNewDbName() + EMPTY_SUFFIX, processWorkers);
            currentTraceGroupWindow = new MapDbPrepperState<>(dbPath, getNewTraceDbName(), processWorkers);
            previousTraceGroupWindow = new MapDbPrepperState<>(dbPath, getNewTraceDbName() + EMPTY_SUFFIX, processWorkers);
        }

        pluginMetrics.gauge(SPANS_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getSpansDbSize());
        pluginMetrics.gauge(TRACE_GROUP_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getTraceGroupDbSize());
    }

    /**
     * This function creates the directory if it doesn't exists and returns the File.
     *
     * @param path
     * @return path
     * @throws RuntimeException if the directory can not be created.
     */
    private static File createPath(File path) {
        if (!path.exists()) {
            if (!path.mkdirs()) {
                throw new RuntimeException(String.format("Unable to create the directory at the provided path: %s", path.getName()));
            }
        }
        return path;
    }

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     *
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceMapRelationship objects representing the edges to be
     * added to the service map index. Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<String>> doExecute(Collection<Record<ExportTraceServiceRequest>> records) {
        final Collection<Record<String>> relationships = windowDurationHasPassed() ? evaluateEdges() : EMPTY_COLLECTION;
        final Map<byte[], ServiceMapStateData> batchStateData = new TreeMap<>(SignedBytes.lexicographicalComparator());
        records.forEach(i -> i.getData().getResourceSpansList().forEach(resourceSpans -> {
            OTelHelper.getServiceName(resourceSpans.getResource()).ifPresent(serviceName -> resourceSpans.getInstrumentationLibrarySpansList().forEach(
                    instrumentationLibrarySpans -> {
                        instrumentationLibrarySpans.getSpansList().forEach(
                                span -> {
                                    if (OTelHelper.checkValidSpan(span)) {
                                        try {
                                            batchStateData.put(
                                                    span.getSpanId().toByteArray(),
                                                    new ServiceMapStateData(
                                                            serviceName,
                                                            span.getParentSpanId().isEmpty() ? null : span.getParentSpanId().toByteArray(),
                                                            span.getTraceId().toByteArray(),
                                                            span.getKind().name(),
                                                            span.getName()));
                                        } catch (RuntimeException e) {
                                            LOG.error("Caught exception trying to put service map state data into batch", e);
                                        }
                                        if (span.getParentSpanId().isEmpty()) {
                                            try {
                                                currentTraceGroupWindow.put(span.getTraceId().toByteArray(), span.getName());
                                            } catch (RuntimeException e) {
                                                LOG.error("Caught exception trying to put trace group name", e);
                                            }
                                        }
                                    } else {
                                        LOG.warn("Invalid span received");
                                    }
                                });
                    }
            ));
        }));
        try {
            currentWindow.putAll(batchStateData);
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to put batch state data", e);
        }
        return relationships;
    }

    /**
     * This function parses the current and previous windows to find the edges, and rotates the window state objects.
     *
     * @return Set of Record<String> containing json representation of ServiceMapRelationships found
     */
    private Collection<Record<String>> evaluateEdges() {
        try {
            final Stream<ServiceMapRelationship> previousStream = previousWindow.iterate(relationshipIterationFunction, preppersCreated.get(), thisPrepperId).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);
            final Stream<ServiceMapRelationship> currentStream = currentWindow.iterate(relationshipIterationFunction, preppersCreated.get(), thisPrepperId).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);

            final Collection<Record<String>> serviceDependencyRecords =
                    Stream.concat(previousStream, currentStream).filter(Objects::nonNull)
                            .filter(serviceMapRelationship -> !relationshipState.contains(serviceMapRelationship))
                            .map(serviceMapRelationship -> {
                                try {
                                    relationshipState.add(serviceMapRelationship);
                                    return new Record<>(OBJECT_MAPPER.writeValueAsString(serviceMapRelationship));
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .collect(Collectors.toSet());

            if (edgeEvaluationLatch == null) {
                initEdgeEvaluationLatch();
            }
            doneEvaluatingEdges();
            waitForEvaluationFinish();

            if (isMasterInstance()) {
                rotateWindows();
                resetWorkState();
            } else {
                waitForRotationFinish();
            }

            return serviceDependencyRecords;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static synchronized void initEdgeEvaluationLatch() {
        if (edgeEvaluationLatch == null) {
            edgeEvaluationLatch = new CountDownLatch(preppersCreated.get());
        }
    }

    /**
     * This function is used to iterate over the current window and find parent/child relationships in the current and
     * previous windows.
     */
    private final BiFunction<byte[], ServiceMapStateData, Stream<ServiceMapRelationship>> relationshipIterationFunction = new BiFunction<byte[], ServiceMapStateData, Stream<ServiceMapRelationship>>() {
        @Override
        public Stream<ServiceMapRelationship> apply(byte[] s, ServiceMapStateData serviceMapStateData) {
            return lookupParentSpan(serviceMapStateData, true);
        }
    };

    private Stream<ServiceMapRelationship> lookupParentSpan(final ServiceMapStateData serviceMapStateData, final boolean checkPrev) {
        if (serviceMapStateData.parentSpanId != null) {
            final ServiceMapStateData parentStateData = getParentStateData(serviceMapStateData.parentSpanId, checkPrev);
            final String traceGroupName = getTraceGroupName(serviceMapStateData.traceId);
            if (traceGroupName != null && parentStateData != null && !parentStateData.serviceName.equals(serviceMapStateData.serviceName)) {
                return Stream.of(
                        ServiceMapRelationship.newDestinationRelationship(parentStateData.serviceName, parentStateData.spanKind, serviceMapStateData.serviceName, serviceMapStateData.name, traceGroupName),
                        //This extra edge is added for compatibility of the index for both stateless and stateful preppers
                        ServiceMapRelationship.newTargetRelationship(serviceMapStateData.serviceName, serviceMapStateData.spanKind, serviceMapStateData.serviceName, serviceMapStateData.name, traceGroupName)
                );
            }
        }
        return Stream.empty();
    }

    /**
     * Checks both current and previous windows for the given parent span id
     *
     * @param spanId
     * @return ServiceMapStateData for the parent span, if exists. Otherwise null
     */
    private ServiceMapStateData getParentStateData(final byte[] spanId, final boolean checkPrev) {
        try {
            final ServiceMapStateData serviceMapStateData = currentWindow.get(spanId);
            return serviceMapStateData != null ? serviceMapStateData : checkPrev ? previousWindow.get(spanId) : null;
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to get parent state data", e);
            return null;
        }
    }

    /**
     * Checks both current and previous trace group windows for the trace id
     *
     * @param traceId
     * @return Trace group name for the given trace if it exists. Otherwise null.
     */
    private String getTraceGroupName(final byte[] traceId) {
        try {
            final String traceGroupName = currentTraceGroupWindow.get(traceId);
            return traceGroupName != null ? traceGroupName : previousTraceGroupWindow.get(traceId);
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to get trace group name", e);
            return null;
        }
    }


    @Override
    public void prepareForShutdown() {
        previousTimestamp = 0L;
    }

    @Override
    public boolean isReadyForShutdown() {
        return currentWindow.size() == 0;
    }

    @Override
    public void shutdown() {
        previousWindow.delete();
        currentWindow.delete();
        previousTraceGroupWindow.delete();
        currentTraceGroupWindow.delete();
    }

    // TODO: Temp code, complex instance creation logic should be moved to a separate class
    static void resetStaticCounters() {
        preppersCreated.set(0);
        edgeEvaluationLatch = null;
    }

    /**
     * Indicate/notify that this instance has finished evaluating edges
     */
    private void doneEvaluatingEdges() {
        edgeEvaluationLatch.countDown();
    }

    /**
     * Wait on all instances to finish evaluating edges
     *
     * @throws InterruptedException
     */
    private void waitForEvaluationFinish() throws InterruptedException {
        edgeEvaluationLatch.await();
    }

    /**
     * Indicate that window rotation is complete
     */
    private void doneRotatingWindows() {
        windowRotationLatch.countDown();
    }

    /**
     * Wait on window rotation to complete
     *
     * @throws InterruptedException
     */
    private void waitForRotationFinish() throws InterruptedException {
        windowRotationLatch.await();
    }

    /**
     * Reset state that indicates whether edge evaluation and window rotation is complete
     */
    private void resetWorkState() {
        windowRotationLatch = new CountDownLatch(1);
        edgeEvaluationLatch = new CountDownLatch(preppersCreated.get());
    }

    /**
     * Rotate windows for prepper state
     */
    private void rotateWindows() {
        LOG.debug("Rotating windows at " + clock.instant().toString());
        previousWindow.delete();
        previousTraceGroupWindow.delete();
        previousWindow = currentWindow;
        currentWindow = new MapDbPrepperState<>(dbPath, getNewDbName(), processWorkers);
        previousTraceGroupWindow = currentTraceGroupWindow;
        currentTraceGroupWindow = new MapDbPrepperState<>(dbPath, getNewTraceDbName(), processWorkers);
        previousTimestamp = clock.millis();
        doneRotatingWindows();
    }

    /**
     * @return Spans database size in bytes
     */
    public double getSpansDbSize() {
        return currentWindow.sizeInBytes() + previousWindow.sizeInBytes();
    }

    /**
     * @return Trace group database size in bytes
     */
    public double getTraceGroupDbSize() {
        return currentTraceGroupWindow.sizeInBytes() + previousTraceGroupWindow.sizeInBytes();
    }

    /**
     * @return Next database name
     */
    private String getNewDbName() {
        return "db-" + clock.millis();
    }

    /**
     * @return Next database name
     */
    private String getNewTraceDbName() {
        return "trace-db-" + clock.millis();
    }

    /**
     * @return Boolean indicating whether the window duration has lapsed
     */
    private boolean windowDurationHasPassed() {
        if ((clock.millis() - previousTimestamp) >= windowDurationMillis) {
            return true;
        }
        return false;
    }

    /**
     * Master instance is needed to do things like window rotation that should only be done once
     *
     * @return Boolean indicating whether this object is the master ServiceMapStatefulPrepper instance
     */
    private boolean isMasterInstance() {
        return thisPrepperId == 0;
    }

    private static class ServiceMapStateData implements Serializable {
        public String serviceName;
        public byte[] parentSpanId;
        public byte[] traceId;
        public String spanKind;
        public String name;

        public ServiceMapStateData() {
        }

        public ServiceMapStateData(final String serviceName, final byte[] parentSpanId,
                                   final byte[] traceId,
                                   final String spanKind,
                                   final String name) {
            this.serviceName = serviceName;
            this.parentSpanId = parentSpanId;
            this.traceId = traceId;
            this.spanKind = spanKind;
            this.name = name;
        }
    }
}