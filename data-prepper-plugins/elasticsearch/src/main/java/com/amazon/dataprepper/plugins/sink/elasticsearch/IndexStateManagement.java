package com.amazon.dataprepper.plugins.sink.elasticsearch;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class IndexStateManagement {
    public static boolean checkISMEnabled(final RestHighLevelClient restHighLevelClient) throws IOException {
        final ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        request.includeDefaults(true);
        final ClusterGetSettingsResponse response = restHighLevelClient.cluster().getSettings(request, RequestOptions.DEFAULT);
        final String enabled = response.getSetting(IndexConstants.ISM_ENABLED_SETTING);
        return enabled != null && enabled.equals("true");
    }

    public static String checkAndCreatePolicy(
            final RestHighLevelClient restHighLevelClient, final String indexType) throws IOException {
        if (checkISMEnabled(restHighLevelClient) && indexType.equals(IndexConstants.RAW)) {
            final String endPoint = "/_opendistro/_ism/policies/" + IndexConstants.RAW_ISM_POLICY;
            Request request = new Request(HttpMethod.HEAD, endPoint);
            final Response response = restHighLevelClient.getLowLevelClient().performRequest(request);
            if (response.getStatusLine().getStatusCode() != RestStatus.OK.getStatus()) {
                final InputStream is = IndexStateManagement.class.getClassLoader().getResourceAsStream(IndexConstants.RAW_ISM_FILE);
                assert is != null;
                final StringBuilder policyJsonBuffer = new StringBuilder();
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                    reader.lines().forEach(line -> policyJsonBuffer.append(line).append("\n"));
                }
                is.close();
                request = new Request(HttpMethod.PUT, endPoint);
                request.setJsonEntity(policyJsonBuffer.toString());
                restHighLevelClient.getLowLevelClient().performRequest(request);
            }
            return IndexConstants.RAW_ISM_POLICY;
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static void attachPolicy(
            final IndexConfiguration configuration, final String ismPolicyId, final String rolloverAlias) {
        configuration.getIndexTemplate().putIfAbsent("settings", new HashMap<>());
        // Attach policy_id and rollover_alias
        ((Map<String, Object>) configuration.getIndexTemplate().get("settings")).put(IndexConstants.ISM_POLICY_ID_SETTING, ismPolicyId);
        ((Map<String, Object>) configuration.getIndexTemplate().get("settings")).put(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, rolloverAlias);
    }
}