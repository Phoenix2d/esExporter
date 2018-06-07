package com.electriccloud.esexporter.service;

import com.electriccloud.esexporter.service.helper.EsIndexHelper;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class BootstrapperService {

    private final IndexProcessorService indexProcessorService;

    private final RestClientBuilder awsElasticsearchClientBuilder;
    private final ExecutorService workerPool;
    private final EsIndexHelper esHelper;

    @Autowired
    public BootstrapperService(ExecutorService workerPool, RestClientBuilder elasticsearchRestClientBuilder, IndexProcessorService indexProcessorService, EsIndexHelper esHelper) {
        this.workerPool = workerPool;
        this.awsElasticsearchClientBuilder = elasticsearchRestClientBuilder;
        this.esHelper = esHelper;
        this.indexProcessorService = indexProcessorService;
    }

    @EventListener(ApplicationReadyEvent.class)
    @SneakyThrows(InterruptedException.class)
    public void bootstrap() {
        long start = System.currentTimeMillis();
        List<String> indices = Lists.newArrayList(esHelper.getIndices(esClient()));

        log.warn("available indices {}", indices);

        indices.stream().forEach(this::processIndex);

        workerPool.shutdown();
        workerPool.awaitTermination(5, TimeUnit.MINUTES);
        long duration = System.currentTimeMillis() - start;
        log.error("upload complete in {} seconds", TimeUnit.MILLISECONDS.toSeconds(duration));
    }

    private void processIndex(String index) {
        log.info("processing index: {}", index);
        indexProcessorService.processIndexData(esHelper.streamIndex(index, esClient()), index);
    }

    private RestClient esClient() {
        return awsElasticsearchClientBuilder.build();
    }
}
