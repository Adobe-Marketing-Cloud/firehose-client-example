package com.adobe.example.firehose;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Basic firehose client. Takes two arguments: the stream url and an authentication token.
 */
public class FirehoseClientDriver {

    public static void main(String[] args) {
        final HttpFirehoseClient client = new HttpFirehoseClient();

        String streamUrl = args[0]; // e.g. https://firehose1.omniture.com/api/1/stream/<stream name>
        String accessToken = args[1]; // usually a very large base64-encoded string

        client.setStreamUrl(streamUrl);
        client.setAccessToken(accessToken);
        int numDisconnects = 0;

        while (true) {
            if (numDisconnects > 3) {
                System.err.println("reconnect failed 3 times, exiting");
                break;
            }
            
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(client.getStream()));
                numDisconnects = 0;
                try {
                    String line = reader.readLine();
                    while (line != null) {
                        if (line.length() > 0) {
                            System.out.println(line);
                        }
                        line = reader.readLine();
                    }
                } catch (IOException e) {
                    // try reconnecting
                    System.err.println("connection dropped: " + e.getMessage());
                }

                System.err.println("will try reconnecting in 5s");
                Thread.sleep(5000);
                numDisconnects++;
            } catch (IOException e) {
                System.err.println("unable to get stream "
                        + client.getStreamUrl() + ": " + e.getMessage());
                break;
            } catch (InterruptedException e) {
                System.err.println("interrupted!");
                break;
            } finally {
                client.close();
            }

        }
    }
}
