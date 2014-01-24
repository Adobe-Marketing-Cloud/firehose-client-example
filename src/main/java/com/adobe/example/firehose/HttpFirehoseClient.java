package com.adobe.example.firehose;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class HttpFirehoseClient {

    private String accessToken;
    private String streamUrl;
    private HttpURLConnection connection;

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public InputStream getStream() throws IOException {
        URL url = new URL(streamUrl);
        connection = (HttpURLConnection) url.openConnection();
        connection.setReadTimeout(1000 * 60 * 60);
        connection.setConnectTimeout(1000 * 10);

        if (accessToken != null) {
            connection.setRequestProperty("Authorization", "Bearer "
                    + accessToken);
        }

        connection.setRequestProperty("Accept-Encoding", "gzip");

        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException(connection.getResponseMessage() + " ("
                    + connection.getResponseCode() + ")");
        }

        InputStream inputStream = new StreamingGZIPInputStream(
                connection.getInputStream());

        return inputStream;
    }

    public void close() {
        if (connection != null) {
            connection.disconnect();
        }
    }

    public void setStreamUrl(String streamUrl) {
        this.streamUrl = streamUrl;
    }

    public String getStreamUrl() {
        return streamUrl;
    }

    public static class StreamingGZIPInputStream extends GZIPInputStream {
        private final InputStream wrapped;

        public StreamingGZIPInputStream(InputStream is) throws IOException {
            super(is);
            wrapped = is;
        }

        public int available() throws IOException {
            return wrapped.available();
        }
    }
}
