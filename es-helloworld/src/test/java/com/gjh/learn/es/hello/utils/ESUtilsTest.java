package com.gjh.learn.es.hello.utils;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * created on 2021/3/13
 *
 * @author kevinlights
 */
@SpringBootTest
@RunWith(SpringRunner.class)
class ESUtilsTest {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9200;
    private static final String SCHEME = "http";

    @Autowired
    ESUtils esUtils;

    @Test
    void createIndex() {
        esUtils.createIndex("test");
    }

    @Test
    void getIndices() {
        esUtils.getIndices();
    }

    @Test
    void createDoc() {
        esUtils.createDoc("test", "testDoc", "1", "{\"name\": \"testName\"}");
    }

    @Test
    void searchDoc() {
        esUtils.searchDoc("test", "{\"query\": {\"match_all\": {}}}");
    }


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
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);

        builder.setFailureListener(new RestClient.FailureListener() {
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

        Cancellable cancellable = client.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                System.out.println(response.toString());
            }

            @Override
            public void onFailure(Exception exception) {
                System.out.println(exception.toString());
            }
        });
        // cancellable.cancel();

        request.addParameter("pretty", "true");
        request.setEntity(new NStringEntity("{\"json\":\"text\"}", ContentType.APPLICATION_JSON));
        request.setJsonEntity("{\"json\":\"text\"}");

        RequestOptions.Builder requestBuilder = RequestOptions.DEFAULT.toBuilder();
        requestBuilder.addHeader("Authorization", "Bearer token");
        requestBuilder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        RequestOptions options = requestBuilder.build();

        request.setOptions(options);
    }

    @Test
    void testAsync() throws InterruptedException {
        RestClientBuilder builder = RestClient.builder(new HttpHost(HOST, PORT, SCHEME));
        RestClient client = builder.build();

        final CountDownLatch latch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            Request request = new Request("PUT", "/ports/doc/" + i);
            request.setEntity(new NStringEntity("{\"json\":\"text\"}", ContentType.APPLICATION_JSON));
            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    latch.countDown();
                    System.out.println(latch.getCount());
                    RequestLine requestLine = response.getRequestLine();
                    HttpHost host = response.getHost();
                    response.getStatusLine().getStatusCode();
                    Header[] headers = response.getHeaders();
                    try {
                        String responseBody = EntityUtils.toString(response.getEntity());
                        System.out.println(responseBody);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailure(Exception exception) {
                    latch.countDown();
                    System.out.println(latch.getCount());
                }
            });
        }
        latch.await();
    }

    @Test
    void testAuthentication() throws IOException {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user", "password"));
        RestClientBuilder builder = RestClient.builder(new HttpHost(HOST, PORT, SCHEME))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.disableAuthCaching();
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                });
        RestClient client = builder.build();
        client.close();
    }
}