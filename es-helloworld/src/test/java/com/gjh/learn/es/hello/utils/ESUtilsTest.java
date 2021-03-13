package com.gjh.learn.es.hello.utils;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * created on 2021/3/13
 *
 * @author kevinlights
 */
class ESUtilsTest {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9200;
    private static final String SCHEME = "http";

    @Test
    void testClient() throws IOException {
        RestClient client = RestClient.builder(
                new HttpHost(HOST, PORT, SCHEME)
        ).build();
        assertTrue(client.isRunning());
        assertEquals(1, client.getNodes().size());
        client.close();
    }

    @Test
    void testBuilder() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(HOST, PORT, SCHEME));
        Header[] defaultHeaders = new Header[] {new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);

        builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
            }
        });

        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(1000));

        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setProxy(new HttpHost("127.0.0.1", 8080, "http")));
    }

    @Test
    void testRequest() throws IOException {
        RestClientBuilder builder = RestClient.builder(new HttpHost(HOST, PORT, SCHEME));
        RestClient client = builder.build();
        Request request = new Request("GET", "/");
        Response response = client.performRequest(request);
        System.out.println(response.toString());
        HttpHost host = response.getHost();
        System.out.println(host.getHostName());
    }
}