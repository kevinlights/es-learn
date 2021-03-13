package com.gjh.learn.es.hello.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * created on 2021/3/13
 *
 * @author kevinlights
 */
@Component
public class ESUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ESUtils.class);

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.post}")
    private int post;

    @Value("${elasticsearch.scheme}")
    private String scheme;

    public RestClient getClient() {
        return RestClient.builder(new HttpHost(host, post, scheme)).build();
    }

    public void createIndex(String idxName) {
        LOG.info("createIndex -> {}", idxName);
        Request request = getRequest("PUT", String.format("/%s", idxName), null, null);
        try {
            Response response = getClient().performRequest(request);
            String respBody = EntityUtils.toString(response.getEntity());
            LOG.info("createIndex -> success: \n{}", respBody);
        } catch (IOException e) {
            LOG.error("createIndex -> error: ", e);
        }
    }

    public void getIndices() {
        LOG.info("getIndices");
        Request request = getRequest("GET", "/_cat/indices", null, null);
        try {
            Response response = getClient().performRequest(request);
            String respBody = EntityUtils.toString(response.getEntity());
            LOG.info("getIndices -> success: \n{}", respBody);
        } catch (IOException e) {
            LOG.error("getIndices -> error: ", e);
        }
    }

    public void createDoc(String idxName, String docName, String docId, String docJson) {
        LOG.info("createDoc -> {}, {}, {}, {}", idxName, docName, docId, docJson);
        Request request = getRequest("PUT", String.format("/%s/%s/%s", idxName, docName, docId), new NStringEntity(docJson, ContentType.APPLICATION_JSON), null);
        try {
            Response response = getClient().performRequest(request);
            String respBody = EntityUtils.toString(response.getEntity());
            LOG.info("createDoc -> success: \n{}", respBody);
        } catch (IOException e) {
            LOG.error("createDoc -> error: ", e);
        }
    }

    public void searchDoc(String idxName, String queryJson) {
        LOG.info("searchDoc -> {}, {}", idxName, queryJson);
        Request request = getRequest("GET", String.format("/%s/_search", idxName), new NStringEntity(queryJson, ContentType.APPLICATION_JSON), null);
        try {
            Response response = getClient().performRequest(request);
            String respBody = EntityUtils.toString(response.getEntity());
            LOG.info("searchDoc -> success: \n{}", respBody);
        } catch (IOException e) {
            LOG.error("searchDoc -> error: ", e);
        }
    }

    public Request getRequest(String method, String endpoint, HttpEntity entity, Map<String, String> params) {
        Request request = new Request(method, endpoint);
        if (null != entity) {
            request.setEntity(entity);
        }
        if (null != params) {
            request.addParameters(params);
        }
        return request;
    }

}
