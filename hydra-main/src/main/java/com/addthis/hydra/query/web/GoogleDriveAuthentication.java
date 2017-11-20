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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import java.net.URI;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.Parameter;

import com.addthis.maljson.JSONObject;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.util.CharsetUtil;

public class GoogleDriveAuthentication {

    private static final String gdriveClientId = Parameter.value("qmaster.export.gdrive.clientId");
    private static final String gdriveClientSecret = Parameter.value("qmaster.export.gdrive.clientSecret");
    private static final boolean gdriveEnabled = Parameter.boolValue("qmaster.export.gdrive.enable", true);
    private static final String gdriveDomain = Parameter.value("qmaster.export.domain.suffix");

    static final String autherror = "autherror";
    static final String authtoken = "authtoken";

    private static final String hostname = System.getenv("HOSTNAME");

    private static final Logger log = LoggerFactory.getLogger(GoogleDriveAuthentication.class);

    /**
     * If a resource a non-null then close the resource. Catch any IOExceptions and log them.
     */
    private static void closeResource(@Nullable Closeable resource) {
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
    private static void sendErrorMessage(ChannelHandlerContext ctx, String message) throws IOException {
        try (StringBuilderWriter writer = new StringBuilderWriter(50)) {
            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(CONTENT_TYPE, "text/html; charset=utf-8");
            writer.append("<html><head><title>Hydra Query Master</title></head><body>");
            writer.append("<h3>");
            writer.append(message);
            writer.append("</h3></body></html>");
            ByteBuf textResponse = ByteBufUtil.encodeString(ctx.alloc(),
                CharBuffer.wrap(writer.getBuilder()), CharsetUtil.UTF_8);
            HttpContent content = new DefaultHttpContent(textResponse);
            response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, textResponse.readableBytes());
            ctx.write(response);
            ctx.write(content);
            ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * (1) If we cannot determine the hostname then return "localhost".
     * (2) If the result returned from the HOSTNAME environment variable
     * is a fully qualified name and the "qmaster.export.domain.suffix"
     * system property has been set then rewrite the hostname.
     * (3) Otherwise return the value from the HOSTNAME environment variable.
     *
     * @return hostname
     */
    private static String generateTargetHostName() {
        String result;
        if (hostname == null) {
            result = "localhost";
        } else {
            int index = hostname.indexOf('.');
            if (index >= 0 && gdriveDomain != null) {
                result = hostname.substring(0, index) + gdriveDomain;
            } else {
                result = hostname;
            }
        }
        return result;
    }

    /**
     * Obtain a Google authorization token. This token is worthless by itself. It
     * is an intermediate step to obtain an access token. We need to do these two
     * steps because...reasons.
     */
    static void gdriveAuthorization(KVPairs kv, ChannelHandlerContext ctx) throws Exception {
        if (gdriveClientId == null && gdriveClientSecret == null) {
            sendErrorMessage(ctx, "The system properties \"qmaster.export.gdrive.clientId\"" +
                                  " and \"qmaster.export.gdrive.clientSecret\" are both null.");
            return;
        } else if (gdriveClientId == null) {
            sendErrorMessage(ctx, "The system property \"qmaster.export.gdrive.clientId\"" +
                                  " is null.");
            return;
        } else if (gdriveClientSecret == null) {
            sendErrorMessage(ctx, "The system property \"qmaster.export.gdrive.clientSecret\"" +
                                  " is null.");
            return;
        } else if (!gdriveEnabled) {
            sendErrorMessage(ctx, "The system property \"qmaster.export.gdrive.enable\"" +
                                  " is false.");
            return;
        }
        QueryStringEncoder encoder = new QueryStringEncoder("");
        Iterator<KVPair> iterator = kv.iterator();
        while(iterator.hasNext()) {
            KVPair pair = iterator.next();
            encoder.addParam(pair.getKey(), pair.getValue());
        }
        String state = encoder.toString().substring(1);
        URI uri = new URIBuilder()
                .setScheme("https")
                .setHost("accounts.google.com")
                .setPath("/o/oauth2/auth")
                .setParameter("scope", "https://www.googleapis.com/auth/drive.file")
                .setParameter("state", state)
                .setParameter("redirect_uri", "http://" + generateTargetHostName() + ":2222/query/google/submit")
                .setParameter("response_type", "code")
                .setParameter("client_id", gdriveClientId)
                .build();
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FOUND);
        response.headers().set(HttpHeaders.Names.LOCATION, uri);
        ctx.write(response);
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        log.trace("response pending");
        log.trace("Setting close listener");
        lastContentFuture.addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Use the Google authorization token to obtain a Google access token.
     * Google OAuth2 your documentation is sorely lacking.
     *
     * @param kv store the access token as a (key, value) pair
     * @return true if the access token was retrieved
     */
    static boolean gdriveAccessToken(KVPairs kv, ChannelHandlerContext ctx) throws Exception {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        if (kv.hasKey(autherror)) {
            sendErrorMessage(ctx, "Error while attempting to authorize google drive access: " +
                                  kv.getValue(autherror));
            return false;
        } else if (!kv.hasKey(authtoken)) {
            sendErrorMessage(ctx, "Error while attempting to authorize google drive access: " +
                                  "authorization token is missing.");
            return false;
        }
        try {
            String code = kv.getValue(authtoken);
            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost("https://accounts.google.com/o/oauth2/token");
            httpPost.setHeader(HttpHeaders.Names.CONTENT_TYPE, URLEncodedUtils.CONTENT_TYPE);
            Set<NameValuePair> parameters = new HashSet<>();
            // Why is this redirect_uri required??? It appeared to be unused by the protocol.
            parameters.add(new BasicNameValuePair("redirect_uri", "http://" + generateTargetHostName() + ":2222/query/google/submit"));
            parameters.add(new BasicNameValuePair("code", code));
            parameters.add(new BasicNameValuePair("client_id", gdriveClientId));
            parameters.add(new BasicNameValuePair("client_secret", gdriveClientSecret));
            parameters.add(new BasicNameValuePair("grant_type", "authorization_code"));
            httpPost.setEntity(new StringEntity(URLEncodedUtils.format(parameters, Charset.defaultCharset()),
                    StandardCharsets.UTF_8));
            httpResponse = httpClient.execute(httpPost);
            if (httpResponse.getStatusLine().getStatusCode() != HttpResponseStatus.OK.code()) {
                sendErrorMessage(ctx, "Error while attempting to exchange the authorization token " +
                                      "for the access token: " + httpResponse.getStatusLine().getReasonPhrase());
                return false;
            }
            String responseEntity = EntityUtils.toString(httpResponse.getEntity());
            JSONObject response = new JSONObject(responseEntity);
            if (response.has("error")) {
                sendErrorMessage(ctx, "Error while attempting to exchange the authorization token " +
                                      "for the access token: " + response.getString("error_description"));
                return false;
            } else if (!response.has("access_token")) {
                sendErrorMessage(ctx, "Error while attempting to exchange the authorization token " +
                                      "for the access token: No access token received.");
                return false;
            }
            kv.addValue("accesstoken", response.getString("access_token"));
            return true;
        } finally {
            closeResource(httpResponse);
            closeResource(httpClient);
        }
    }

}
