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
 *
 * Copyright 2012 The Netty Project
 *
 * Contents have been modified
 */
package com.addthis.hydra.query.web;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Pattern;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;

import com.addthis.basis.util.Parameter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.hydra.query.web.HttpUtils.sendError;
import static com.addthis.hydra.query.web.HttpUtils.sendNotModified;
import static com.addthis.hydra.query.web.HttpUtils.setContentTypeHeader;
import static com.addthis.hydra.query.web.HttpUtils.setDateAndCacheHeaders;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.IF_MODIFIED_SINCE;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import io.netty.handler.codec.http.HttpResponse;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * A simple handler that serves incoming HTTP requests to send their respective
 * HTTP responses.  It also implements {@code 'If-Modified-Since'} header to
 * take advantage of browser cache, as described in
 * <a href="http://tools.ietf.org/html/rfc2616#section-14.25">RFC 2616</a>.
 * <p/>
 * <h3>How Browser Caching Works</h3>
 * <p/>
 * Web browser caching works with HTTP headers as illustrated by the following
 * sample:
 * <ol>
 * <li>Request #1 returns the content of {@code /file1.txt}.</li>
 * <li>Contents of {@code /file1.txt} is cached by the browser.</li>
 * <li>Request #2 for {@code /file1.txt} does return the contents of the
 * file again. Rather, a 304 Not Modified is returned. This tells the
 * browser to use the contents stored in its cache.</li>
 * <li>The server knows the file has not been modified because the
 * {@code If-Modified-Since} date is the same as the file's last
 * modified date.</li>
 * </ol>
 * <p/>
 * <pre>
 * Request #1 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 *
 * Response #1 Headers
 * ===================
 * HTTP/1.1 200 OK
 * Date:               Tue, 01 Mar 2011 22:44:26 GMT
 * Last-Modified:      Wed, 30 Jun 2010 21:36:48 GMT
 * Expires:            Tue, 01 Mar 2012 22:44:26 GMT
 * Cache-Control:      private, max-age=31536000
 *
 * Request #2 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 * If-Modified-Since:  Wed, 30 Jun 2010 21:36:48 GMT
 *
 * Response #2 Headers
 * ===================
 * HTTP/1.1 304 Not Modified
 * Date:               Tue, 01 Mar 2011 22:44:28 GMT
 *
 * </pre>
 */
@ChannelHandler.Sharable
public class HttpStaticFileHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(HttpStaticFileHandler.class);
    private static final String WEB_RESOURCE_DIR = "web";

    private final ClassLoader classLoader;
    private final Path jarPath;
    private final long jarModifiedSeconds;


    public HttpStaticFileHandler() {
        this.classLoader = this.getClass().getClassLoader();
        try {
            jarPath  = Paths.get(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
            jarModifiedSeconds = Files.getLastModifiedTime(jarPath).toMillis() / 1000;
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (!request.getDecoderResult().isSuccess()) {
            sendError(ctx, BAD_REQUEST);
            return;
        }
        // since we are using send file, we must remove the compression unit or it will donk out
        ChannelHandler compressor = ctx.pipeline().get("compressor");
        if (compressor != null) {
            ctx.pipeline().remove("compressor");
        }

        if (request.getMethod() != GET) {
            sendError(ctx, METHOD_NOT_ALLOWED);
            return;
        }

        QueryStringDecoder urlDecoder = new QueryStringDecoder(request.getUri());
        String target = urlDecoder.path();

        final String path = sanitizeUri(target);
        if (path == null) {
            sendError(ctx, FORBIDDEN);
            return;
        }

        log.trace("cache validation occuring for {}", path);
        // Cache Validation
        String ifModifiedSince = request.headers().get(IF_MODIFIED_SINCE);
        if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(HttpUtils.HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);

            // Only compare up to the second because the datetime format we send to the client
            // does not have milliseconds
            long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
            if (ifModifiedSinceDateSeconds == jarModifiedSeconds) {
                sendNotModified(ctx);
                return;
            }
        }

        log.trace("reading {}", path);
        Path resourcePath = Paths.get(WEB_RESOURCE_DIR, path);
        InputStream resourceStream = classLoader.getResourceAsStream(resourcePath.toString());
        int fileLength = -1;
        try {
            fileLength = resourceStream.available();
        } catch(Exception e) {
            log.trace("resource \"{}\" not readable: {}", resourcePath.toString(), e);
        }

        if (resourceStream == null) {
            sendError(ctx, NOT_FOUND);
            return;
        }
        if (Files.isHidden(resourcePath) || fileLength < 0) {
            resourceStream.close();
            sendError(ctx, NOT_FOUND);
            return;
        }

        ByteBuf resourceContents = UnpooledByteBufAllocator.DEFAULT.buffer(fileLength, fileLength);
        while(resourceStream.available() > 0) {
            resourceContents.writeBytes(resourceStream, resourceStream.available());
        }
        resourceStream.close();

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        setContentLength(response, fileLength);
        setContentTypeHeader(response, resourcePath);
        try {
            setDateAndCacheHeaders(response, jarPath);
        } catch (IOException ioex) {
            sendError(ctx, NOT_FOUND);
            return;
        }
        if (isKeepAlive(request)) {
            response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        // Write the initial line and the header.
        ctx.write(response);

        // Write the content.
        ctx.write(resourceContents);

        // Write the end marker
        ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

        // Decide whether to close the connection or not.
        if (!isKeepAlive(request)) {
            // Close the connection when the whole content is written out.
            lastContentFuture.addListener(ChannelFutureListener.CLOSE);
        } else {
            ctx.pipeline().remove(this);
            if (compressor != null) {
                ctx.pipeline().addBefore("query", "compressor", compressor);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("Exception caught while serving static files", cause);
        if (ctx.channel().isActive()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
    }

    private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

    private static String sanitizeUri(String uri) {

        if (!uri.startsWith("/")) {
            return null;
        }

        // Convert file separators.
        uri = uri.replace('/', File.separatorChar);

        // Simplistic dumb security check.
        // You will have to do something serious in the production environment.
        if (uri.contains(File.separator + '.') ||
            uri.contains('.' + File.separator) ||
            uri.startsWith(".") || uri.endsWith(".") ||
            INSECURE_URI.matcher(uri).matches()) {
            return null;
        }

        return uri;
    }

}
