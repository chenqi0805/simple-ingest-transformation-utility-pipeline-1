package com.amazon.ti.plugins.source.apmtracesource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ApmSpanProcessor {

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final static String INSTRUMENTATION_LIBRARY_SPANS = "instrumentationLibrarySpans";
  private final static String INSTRUMENTATION_LIBRARY = "instrumentationLibrary";
  private final static String SPANS = "spans";
  private final static String RESOURCE = "resource";
  private final static String ATTRIBUTES = "attributes";


  public static ArrayList<String> decodeResourceSpan(final String jsonResourceSpans) throws JsonProcessingException {
    final JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonResourceSpans);
    final ArrayList<String> result = new ArrayList<>(Collections.emptyList());
    //if number of spans is zero, return empty result.
    if(!jsonNode.path(INSTRUMENTATION_LIBRARY_SPANS).isArray())
       return result;
    final ArrayNode instrumentationLibrarySpans = (ArrayNode) jsonNode.path(INSTRUMENTATION_LIBRARY_SPANS);
    //Get Resource attributes, if not present we will store the spans without resources objects.
    final ArrayList<ObjectNode> resourceNodes =  jsonNode.path(RESOURCE).path(ATTRIBUTES).isArray() ?
        processKeyValueList((ArrayNode) jsonNode.path(RESOURCE).path(ATTRIBUTES), String.format("%s.%s", RESOURCE, ATTRIBUTES))
        : new ArrayList<>(Collections.emptyList());
    for (int i = 0; i < instrumentationLibrarySpans.size(); i++) {
      final ObjectNode instrumentationLibraryNode = (ObjectNode)instrumentationLibrarySpans.get(i).path(INSTRUMENTATION_LIBRARY);
      final ArrayNode spans = (ArrayNode) instrumentationLibrarySpans.get(i).path(SPANS);
      //if number of spans is zero, return empty result. Note this is a temporary implementation because in the final version we will process
      // the protobuf.
      if(!instrumentationLibrarySpans.get(i).path(SPANS).isArray())
        return new ArrayList<>(Collections.emptyList());
      for (int j = 0; j < spans.size(); j++) {
        final ObjectNode spanNode = (ObjectNode) spans.get(j);
        //Get Span Attributes. Skipping Events/Links for now
        if(spanNode.path(ATTRIBUTES).isArray())
          processKeyValueList((ArrayNode) spanNode.remove(ATTRIBUTES), ATTRIBUTES).forEach(spanNode::setAll);
        resourceNodes.forEach(spanNode::setAll);
        spanNode.setAll(instrumentationLibraryNode);
        result.add(OBJECT_MAPPER.writeValueAsString(spanNode));
      }
    }
    return result;
  }

  public JsonNode processResource(final JsonNode resourceNode) {
      return resourceNode.path(RESOURCE);
  }

  //Note the current version has zero dependency on opentelemetry-proto so we are going to
  //assume attributesKeyValue is processed like below.
  public static ArrayList<ObjectNode> processKeyValueList(final ArrayNode resourceAttributes, final String prefix) {
    final ArrayList<ObjectNode> objectNodes = new ArrayList<>(Collections.emptyList());
    for (int i = 0; i < resourceAttributes.size(); i++) {
      final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      final String newKey =  String.format("%s.%s", prefix,resourceAttributes.get(i).get("key").asText());
      switch (resourceAttributes.get(i).get("value").fieldNames().next()){
        case "stringValue": objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("stringValue").asText())); break;
        case "intValue":  objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("intValue").asLong())); break;
        case "boolValue": objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("boolValue").asBoolean())); break;
        case "doubleValue": objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("doubleValue").asDouble())); break;
        case "arrayValue": objectNodes.add(objectNode.set(newKey, resourceAttributes.get(i).get("value").get("arrayValue"))); break;
        case "keyValueList":
          //TBD: This flatten is something we will not do, but keeping it here.
          objectNodes.addAll(processKeyValueList((ArrayNode) resourceAttributes.get(i).get("value"), newKey));
          break;
      }
    }
    return objectNodes;
  }
}
