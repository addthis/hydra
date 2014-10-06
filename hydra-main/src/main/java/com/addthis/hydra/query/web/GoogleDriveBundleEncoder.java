/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.addthis.hydra.query.web;

import java.io.Closeable;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.nio.charset.StandardCharsets;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.core.Bundle;
import com.addthis.maljson.JSONObject;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

public class GoogleDriveBundleEncoder extends AbstractBufferingHttpBundleEncoder {

    private static final Logger log = LoggerFactory.getLogger(GoogleDriveBundleEncoder.class);

    private static final long bundlePrintInterval = Parameter.longValue("qmaster.export.gdrive.print.bundles", 10);

    private static final int threadCount = Parameter.intValue("qmaster.export.gdrive.threads", 4);

    private static final ExecutorService executor = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(threadCount, threadCount,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("goodle-drive-%d").build()),
            5, TimeUnit.SECONDS);

    /**
     * Name of the new file in the Google filesystem.
     */
    private final String filename;

    /**
     * OAuth2 access token with permissions to create and update new files.
     */
    private final String accessToken;

    /**
     * Input stream used by the HTTP request to send the file to Google.
     */
    private final PipedInputStream inputStream;

    /**
     * Output stream that is streaming the query results.
     */
    private final PrintStream printStream;

    private final Future<CloseableHttpResponse> googleResponse;

    private final CloseableHttpClient httpClient;

    /**
     * Number of rows processed.
     */
    private long count;

    private class GoogleRequest implements Callable<CloseableHttpResponse> {

        /**
         * Create a chunked HTTP POST request that sends the file contents to Google.
         */
        @Override
        public CloseableHttpResponse call() throws URISyntaxException, IOException {
            InputStreamEntity body = new InputStreamEntity(inputStream);
            body.setChunked(true);
            // Parameters
            List<NameValuePair> parameters = new ArrayList<>();
            // This request contains the contents of the file with no metadata
            parameters.add(new BasicNameValuePair("uploadType", "media"));
            // Convert the csv file to Google spreadsheets
            parameters.add(new BasicNameValuePair("convert", "true"));
            URI uri = new URIBuilder().setScheme("https")
                    .setHost("www.googleapis.com")
                    .setPath("/upload/drive/v2/files")
                    .setParameters(parameters).build();
            HttpPost httpPost = new HttpPost(uri);
            httpPost.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/csv");
            // Secret code
            httpPost.setHeader(HttpHeaders.Names.AUTHORIZATION, "Bearer " + accessToken);
            httpPost.setEntity(body);
            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            return httpResponse;
        }
    }

    private GoogleDriveBundleEncoder(String filename, String accessToken) throws IOException {
        super(50, 1); // aggressively flush updates on stream progress
        this.filename = filename;
        this.accessToken = accessToken;
        this.httpClient = HttpClients.createDefault();
        this.inputStream = new PipedInputStream();
        this.printStream = new PrintStream(new PipedOutputStream(inputStream), true, StandardCharsets.UTF_8.name());
        this.googleResponse = executor.submit(new GoogleRequest());
        setContentTypeHeader(responseStart, "text/html; charset=utf-8");
    }

    public static GoogleDriveBundleEncoder create(String filename, String accessToken) throws IOException {
        if (!filename.toLowerCase().endsWith(".csv")) {
            filename = filename.concat(".csv");
        }
        return new GoogleDriveBundleEncoder(filename, accessToken);
    }

    @Override
    protected void appendResponseStartToString(StringBuilder sendBuffer) {
        sendBuffer.append("Begin sending rows to Google drive.<br>");
    }

    @Override
    public void appendBundleToString(Bundle row, StringBuilder sendBuffer) {
        if (++count % bundlePrintInterval == 0) {
            sendBuffer.append("Sending row " + count + " to Google drive.<br>");
        }
    }

    @Override
    public void send(ChannelHandlerContext ctx, Bundle row) {
        super.send(ctx, row);
        printStream.print(DelimitedBundleEncoder.buildRow(row, ","));
    }

    /**
     * If a resource a non-null then close the resource. Catch any IOExceptions and log them.
     */
    private static void closeResource(Closeable resource) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (IOException ex) {
            log.error("Error", ex);
        }
    }

    /**
     * Send an HTML formatted error message.
     */
    private static void writeErrorMessage(ChannelHandlerContext ctx,
            CloseableHttpResponse httpResponse, String message, String body) {
        ctx.write(message);
        ctx.write(httpResponse.getStatusLine().getReasonPhrase());
        ctx.writeAndFlush("<br>");
        ctx.write("Status code ");
        ctx.write(String.valueOf(httpResponse.getStatusLine().getStatusCode()));
        ctx.writeAndFlush("<br>");
        ctx.write(body);
        ctx.writeAndFlush("<br>");
    }

    /**
     * Send a second request to set the filename for the new file.
     *
     * @param ctx
     * @param fileId Identifier to locate the file in the Google Drive
     * @throws Exception
     */
    private void setDocumentFilename(ChannelHandlerContext ctx, String fileId)
            throws IOException, URISyntaxException {

        CloseableHttpResponse httpResponse = null;
        try {
            String contents = "{ \"title\" : " + JSONObject.quote(filename) + "}";
            HttpEntity body = new StringEntity(contents, StandardCharsets.UTF_8);
            // Parameters
            URI uri = new URIBuilder().setScheme("https")
                    .setHost("www.googleapis.com")
                    .setPath("/drive/v2/files/" + fileId)
                    .setParameter("newRevision", "false")
                    .build();
            HttpPut httpPut = new HttpPut(uri);
            httpPut.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=UTF-8");
            // Secret code
            httpPut.setHeader(HttpHeaders.Names.AUTHORIZATION, "Bearer " + accessToken);
            httpPut.setEntity(body);
            httpResponse = httpClient.execute(httpPut);
            String responseEntity = EntityUtils.toString(httpResponse.getEntity());
            if (httpResponse.getStatusLine().getStatusCode() != HttpResponseStatus.OK.code()) {
                writeErrorMessage(ctx, httpResponse,
                    "Error while attempting to name file: ", responseEntity);
            }
        } finally {
            closeResource(httpResponse);
        }
    }

    @Override
    public void sendComplete(ChannelHandlerContext ctx) {
        super.sendComplete(ctx);
        printStream.close();
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = googleResponse.get();
            String responseEntity = EntityUtils.toString(httpResponse.getEntity());
            if (httpResponse.getStatusLine().getStatusCode() != HttpResponseStatus.OK.code()) {
                writeErrorMessage(ctx, httpResponse,
                        "Error while attempting to send file: ", responseEntity);
            } else {
                JSONObject response = new JSONObject(responseEntity);
                ctx.writeAndFlush("Completed sending all " + count + " rows to Google drive.<br>");
                ctx.writeAndFlush("Assigning the name " + filename + " to the new file...<br>");
                String fileId = response.getString("id");
                setDocumentFilename(ctx, fileId);
                ctx.writeAndFlush("File " + filename + " created on Google Drive.<br>");
            }
        } catch (Exception ex) {
            ctx.writeAndFlush(ex.toString());
            log.error("Google drive upload error", ex);
        } finally {
            closeResource(httpResponse);
            closeResource(httpClient);
        }
    }
}
