package com.oldtan.neutask.deleteduplicatedata;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @Description: TODO
 * @Author: tanchuyue
 * @Date: 21-2-25
 */
public class DeleteDuplicateData implements Runnable {

    private final TransportClient esClient;

    private final String index;

    private final Set<String> docIdSet;

    public static volatile CountDownLatch latch;

    public DeleteDuplicateData(TransportClient esClient, String index, Set<String> docIdSet) {
        this.esClient = esClient;
        this.index = index;
        this.docIdSet = docIdSet;
    }

    @Override
    public void run() {
        try {
            BulkRequest request = new BulkRequest().timeout(TimeValue.timeValueMinutes(2));
            docIdSet.stream().forEach(id -> request.add(new DeleteRequest(index,index,id)));
            esClient.bulk(request);
        }finally {
            latch.countDown();
        }

    }
}
