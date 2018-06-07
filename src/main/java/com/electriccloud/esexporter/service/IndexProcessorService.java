package com.electriccloud.esexporter.service;

import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.google.common.base.Joiner;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class IndexProcessorService {

    private static final Pattern tidRegex = Pattern.compile("tid-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}");

    private final AmazonS3 s3Client;
    private final ObjectMapper mapper;

    @Autowired
    public IndexProcessorService(AmazonS3ClientBuilder s3ClientBuilder, ObjectMapper mapper) {
        this.s3Client = s3ClientBuilder.build();
        this.mapper = mapper;
    }

    @Async("threadPoolTaskExecutor")
    @SneakyThrows(IOException.class)
    public void processIndexData(Iterable<JsonNode> indexData, String index) {

        String bucket = bucketForIndex(index);
        String path = pathForIndex(index);
        String filename = "logfile.log";

        final StreamTransferManager stm = new StreamTransferManager("k8s-tenant-logs", Joiner.on("/").join(bucket, path, filename), s3Client, 1,
                1, 2, 5);
        final OutputStream ostream = stm.getMultiPartOutputStreams().get(0);
        final SequenceWriter swriter = mapper.writer().writeValues(ostream);
        for (final JsonNode node : indexData) {
            swriter.write(node);
        }
        swriter.flush();
        swriter.close();
        stm.complete();
    }

    private String bucketForIndex(String index) {
        final Matcher indexMatcher = tidRegex.matcher(index);
        if (indexMatcher.find()) {
            return indexMatcher.group();
        } else {
            return "untenanted";
        }
    }

    private String pathForIndex(String index) {
        if (!index.contains("-")) {
            return "irregular";
        }
        return index.substring(index.lastIndexOf("-") + 1).replaceAll("\\.", "/");
    }

    private String filenameForIndexEntry(JsonNode indexEntry) {
        String sourceField = indexEntry.at("/_source/source").asText();
        return sourceField.substring(sourceField.lastIndexOf("/"));
    }
}
