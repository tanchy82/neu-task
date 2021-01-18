package com.oldtan.config;

import lombok.SneakyThrows;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.util.Arrays;

/**
 * @Description: TODO 生产者
 * @Author: tanchuyue
 * @Date: 21-1-18
 */
@Configuration
public class ElasticsearchConfig {

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Value("${elasticsearch.cluster.nodes}")
    private String nodes;

    public static TransportClient esClient = null;

    @Bean
    @SneakyThrows
    public TransportClient getClient() {
        if (esClient == null){
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName) //如果集群的名字不是默认的elasticsearch，需指定
                    .put("client.transport.sniff", true) //自动嗅探
                    .build();
            esClient =  new PreBuiltTransportClient(settings);
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
        return esClient;


            //可用连接设置参数说明
            /*
            cluster.name
                指定集群的名字，如果集群的名字不是默认的elasticsearch，需指定。
            client.transport.sniff
                设置为true,将自动嗅探整个集群，自动加入集群的节点到连接列表中。
            client.transport.ignore_cluster_name
                Set to true to ignore cluster name validation of connected nodes. (since 0.19.4)
            client.transport.ping_timeout
                The time to wait for a ping response from a node. Defaults to 5s.
            client.transport.nodes_sampler_interval
                How often to sample / ping the nodes listed and connected. Defaults to 5s.
            */
    }

}
