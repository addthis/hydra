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
package com.addthis.hydra.task.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.net.HttpURLConnection;
import java.net.URL;

public class HTTPRequest {

    public static String sendHTTPPost(@Nonnull String address,
            @Nonnull String parameters) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(address);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", "" + Integer.toString(parameters.getBytes().length));
            connection.setUseCaches(false);

            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(parameters);
            wr.flush();
            wr.close();

            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            while ((line = rd.readLine()) != null) {
                result.append(line);
                result.append("\n");
            }

        } catch (IOException ex) {
            IOException revisedEx = buildIOException(connection, ex);

            throw revisedEx;
        } finally {
            connection.disconnect();
        }

        return result.toString();
    }


    public static String sendHTTPPost(@Nonnull String address,
            @Nonnull String parameters,
            @Nonnull String username) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(address);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestMethod("POST");
            connection.addRequestProperty("Username", username);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Content-Length", "" + Integer.toString(parameters.getBytes().length));
            connection.setUseCaches(false);

            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(parameters);
            wr.flush();
            wr.close();

            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            while ((line = rd.readLine()) != null) {
                result.append(line);
                result.append("\n");
            }

        } catch (IOException ex) {
            IOException revisedEx = buildIOException(connection, ex);

            throw revisedEx;
        } finally {
            connection.disconnect();
        }

        return result.toString();
    }

    public static String sendHTTPGet(@Nonnull String address, @Nullable String username) throws IOException {
        StringBuilder result = new StringBuilder();
        URL url = new URL(address);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setRequestMethod("GET");
            if (username != null) {
                connection.addRequestProperty("Username", username);
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;

            while ((line = rd.readLine()) != null) {
                result.append(line);
                result.append("\n");
            }

        } catch (IOException ex) {
            IOException revisedEx = buildIOException(connection, ex);

            throw revisedEx;
        } finally {
            connection.disconnect();
        }

        return result.toString();
    }


    public static String sendHTTPGet(@Nonnull String address) throws IOException {
        return sendHTTPGet(address, null);
    }

    /**
     * Generates a more informative error message by using any messages that have
     * been sent in the error stream.
     *
     * @param connection
     * @param ex
     * @return
     * @throws IOException
     */
    private static IOException buildIOException(@Nonnull HttpURLConnection connection,
            @Nonnull IOException ex)
            throws IOException {
        StringBuilder message = new StringBuilder();
        message.append(ex.getMessage());
        message.append(" with message ");

        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
            message.append("[[no message was provided]]");
            return new IOException(message.toString());
        }
        BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
        String line = rd.readLine();

        message.append("\"");
        while (line != null) {
            message.append(line);
            line = rd.readLine();
            if (line != null) {
                message.append("\n");
            }
        }
        message.append("\"");
        return new IOException(message.toString());
    }

}
