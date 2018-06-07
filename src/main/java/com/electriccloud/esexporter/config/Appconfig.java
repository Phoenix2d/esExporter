package com.electriccloud.esexporter.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@EnableAutoConfiguration
@EnableAsync
public class Appconfig {

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public AmazonS3ClientBuilder getClientBuilder(@Value("${REGION}") String region) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider())
                .withRegion(region)
                .withClientConfiguration(clientConfig());
    }

    @Bean
    public ClientConfiguration clientConfig() {
        return new ClientConfiguration().withGzip(true);
    }

    @Bean
    public AWSCredentialsProvider credentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    @Bean
    public RestClientBuilder esRestClient(@Value("${ES_ENDPOINT}") String endpoint, @Value("${ES_ENDPOINT_PORT}") Integer endpointPort) {
        Header[] headers = new Header[] { new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")};
        return RestClient.builder(new HttpHost(endpoint, endpointPort)).setDefaultHeaders(headers);
    }

    @Bean
    public ObjectMapper getMapper() {
        return new ObjectMapper();
    }

    @Bean(name = "threadPoolTaskExecutor")
    public ExecutorService newExecutor() {
        return Executors.newWorkStealingPool();
    }
}
