package com.electriccloud.esexporter.service.helper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ForwardingIterator;
import com.google.common.net.UrlEscapers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class EsIndexHelper {

    private ObjectMapper objectMapper;
    private String searchPayload;

    private static final TypeReference<List<Map<String, String>>> listMapTref = new TypeReference<List<Map<String, String>>>() {};
    private static final Map<String, String> formatJsonParams;
    private final String indexKey = "index";
    private final String scrollId = "/_scroll_id";
    private final String hitsId = "/hits/hits";

    static {
        formatJsonParams = new HashMap<>(1, 1.0f);
        formatJsonParams.put("format", "json");
    }

    @Autowired
    public EsIndexHelper(ObjectMapper objectMapper, @Value("${elastic.sortEntity}") String searchPayload) {
        this.objectMapper = objectMapper;
        this.searchPayload = searchPayload;
    }

    @SneakyThrows(IOException.class)
    public Iterable<String> getIndices(final RestClient esClient) {
        return FluentIterable.from((List<Map<String, String>>)
                objectMapper.readerFor(listMapTref)
                        .readValue(
                                esClient.performRequest("GET", "/_cat/indices", formatJsonParams)
                                        .getEntity()
                                        .getContent()))
                .transform(map -> map.get(indexKey));
    }

    @SneakyThrows(IOException.class)
    private JsonNode startEsScrollForIndex(final RestClient esClient, String indexName) {
        Map<String, String> params = new HashMap<>(formatJsonParams);
        params.put("scroll", "1m");
        return objectMapper.reader()
                .readTree(
                        esClient.performRequest("POST", String.join("", "/", UrlEscapers.urlFragmentEscaper().escape(indexName), "/_search"), params, new StringEntity(searchPayload))
                                .getEntity()
                                .getContent());
    }

    @SneakyThrows
    private JsonNode esScrollTo(final RestClient esClient, String scrollValue) {
        Map<String, String> params = new HashMap<>(formatJsonParams);
        params.put("scroll", "1m");
        params.put("scroll_id", scrollValue);
        return objectMapper.reader()
                .readTree(esClient.performRequest("GET", "/_search/scroll", params)
                        .getEntity()
                        .getContent());
    }

    private String extractScrollId(JsonNode response) {
        return response.at(scrollId).asText();
    }

    private Iterable<JsonNode> extractIndexData(JsonNode response) {
        final JsonNode hitsData = response.at(hitsId);
        if (!hitsData.isArray()) {
            log.error("expected array but got {} for {}", hitsData.getNodeType().toString(), hitsData);
            throw new IllegalStateException("wrong index response " + hitsData.getNodeType().toString() + " instead of array");
        }
        return FluentIterable.from(hitsData::elements);
    }

    public Iterable<JsonNode> streamIndex(String index, RestClient esClient) {
        final JsonNode responseNode = startEsScrollForIndex(esClient, index);

        return () -> new ForwardingIterator<JsonNode>() {

            private Iterator<JsonNode> delegate = extractIndexData(responseNode).iterator();
            private String scrollId = extractScrollId(responseNode);

            @Override
            protected Iterator<JsonNode> delegate() {
                return delegate;
            }

            @Override
            public boolean hasNext() {
                if (!super.hasNext()) {
                    final JsonNode responseNode = esScrollTo(esClient, scrollId);
                    this.scrollId = extractScrollId(responseNode);
                    this.delegate = extractIndexData(responseNode).iterator();
                }
                return super.hasNext();
            }
        };
    }
}
