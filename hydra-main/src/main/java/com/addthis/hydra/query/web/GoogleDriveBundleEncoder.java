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

import java.net.URI;

import java.util.ArrayList;
import java.util.List;


import java.nio.charset.StandardCharsets;

import com.addthis.basis.util.Parameter;

import com.addthis.bundle.core.Bundle;
import com.addthis.maljson.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
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

class GoogleDriveBundleEncoder extends AbstractHttpBundleEncoder {

    private static final Logger log = LoggerFactory.getLogger(GoogleDriveBundleEncoder.class);

    private static final long bundlePrintInterval = Parameter.longValue("qmaster.export.gdrive.print.bundles", 1000);

    private final StringBuilder builder;

    private final String filename;

    private final String accessToken;

    private long count;

    private GoogleDriveBundleEncoder(String filename, String accessToken) {
        super();
        this.builder = new StringBuilder();
        this.filename = filename;
        this.accessToken = accessToken;
        setContentTypeHeader(responseStart, "text/html; charset=utf-8");
    }

    public static GoogleDriveBundleEncoder create(String filename, String accessToken) {
        if (!filename.toLowerCase().endsWith(".csv")) {
            filename = filename.concat(".csv");
        }
        return new GoogleDriveBundleEncoder(filename, accessToken);
    }

    @Override
    public void writeStart(ChannelHandlerContext ctx) {
        super.writeStart(ctx);
    }

    @Override
    public void send(ChannelHandlerContext ctx, Bundle row) {
        super.send(ctx, row);
        builder.append(DelimitedBundleEncoder.buildRow(row, ","));
        if (++count % bundlePrintInterval == 0) {
            ctx.writeAndFlush("Sending row " + count + " to Google drive.<br>");
        }
    }

    private static void closeResource(Closeable resource) {
        try {
            if (resource != null) {
                resource.close();
            }
        } catch (IOException ex) {
            log.error("Error: {}", ex);
        }
    }

    private JSONObject sendFileContents(ChannelHandlerContext ctx, String contents) throws Exception {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        try {
            HttpEntity body = new StringEntity(contents, StandardCharsets.UTF_8);
            httpClient = HttpClients.createDefault();
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
            httpResponse = httpClient.execute(httpPost);
            String responseEntity = EntityUtils.toString(httpResponse.getEntity());
            if (httpResponse.getStatusLine().getStatusCode() != HttpResponseStatus.OK.code()) {
                writeErrorMessage(ctx, httpResponse,
                    "Error while attempting to send file: ", responseEntity);
                return null;
            }
            JSONObject response = new JSONObject(responseEntity);
            return response;
        } finally {
            closeResource(httpResponse);
            closeResource(httpClient);
        }
    }

    private void writeErrorMessage(ChannelHandlerContext ctx,
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

    private void setDocumentFilename(ChannelHandlerContext ctx, String fileId) throws Exception {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        try {
            String contents = "{ \"title\" : " + JSONObject.quote(filename) + "}";
            HttpEntity body = new StringEntity(contents, StandardCharsets.UTF_8);
            httpClient = HttpClients.createDefault();
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
            closeResource(httpClient);
        }
    }

    @Override
    public void sendComplete(ChannelHandlerContext ctx) {
        super.sendComplete(ctx);
        try {
            JSONObject response = sendFileContents(ctx, builder.toString());
            if (response != null) {
                ctx.writeAndFlush("Completed sending all " + count + " rows to Google drive.<br>");
                ctx.writeAndFlush("Assigning the name " + filename + " to the new file...<br>");
                String fileId = response.getString("id");
                setDocumentFilename(ctx, fileId);
                ctx.writeAndFlush("File " + filename + " created on Google Drive.<br>");
            }
        } catch (Exception ex) {
            ctx.writeAndFlush(ex.toString());
            log.error("Google drive upload error", ex);
        }
    }
}
