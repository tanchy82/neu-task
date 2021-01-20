package com.oldtan.config;

import lombok.SneakyThrows;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description: elasticsearch config
 * @Author: tanchuyue
 * @Date: 21-1-18
 */
@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Value("${elasticsearch.cluster.nodes}")
    private String nodes;

    private static volatile TransportClient esClient = null;

    private ReentrantLock lock = new  ReentrantLock();

    @SneakyThrows
    public TransportClient getClient() {
        if (esClient == null) {
            lock.lock();
            try {
                if (esClient == null) {
                    esClient = new PreBuiltTransportClient(Settings.builder().put("cluster.name", clusterName)
                            .put("client.transport.sniff", true).build());
                    Arrays.stream(nodes.split(",")).forEach((s) -> {
                        try {
                            esClient.addTransportAddress(
                                    new InetSocketTransportAddress(InetAddress.getByName(s.split(":")[0]),
                                            Integer.parseInt(s.split(":")[1])));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    });
                }
            }finally {
                lock.unlock();
            }

        }
        return esClient;
    }

}
