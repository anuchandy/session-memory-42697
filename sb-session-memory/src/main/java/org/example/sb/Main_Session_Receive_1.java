package org.example.sb;

import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import java.time.Duration;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main_Session_Receive_1 {
    private static final String CON_STR = System.getenv("CON_STR");
    private static final String Q_NAME = System.getenv("Q_NAME");

    public static void main(String[] args) {
        // 1. Create a ServiceBusClientBuilder
        final ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
                .connectionString(CON_STR);

        int i = 0;
        while (i < 1000000) {
            System.out.println("Creating ServiceBusSessionReceiverClient #" + i);
            // 2. Create a ServiceBusSessionReceiverClient
            ServiceBusSessionReceiverClient sessionClient = clientBuilder
                    .sessionReceiver()
                    .queueName(Q_NAME)
                    .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                    .disableAutoComplete()
                    .prefetchCount(0)
                    .buildClient();

            ServiceBusReceiverClient receiverClient ;
            try {
                // 3. Obtain a session using ServiceBusSessionReceiverClient.acceptNextSession()
                receiverClient  = sessionClient.acceptNextSession();
            } catch (Exception e) {
                // 3.1 If we get a timeout with no active session , call ServiceBusSessionReceiverClient.close() and
                //     continue to create a new ServiceBusSessionReceiverClient
                close("close(sessionClient) acceptNextSession-error.", sessionClient);
                sleep(250);
                continue;
            }

            boolean receiverErrorClosed = false;
            try {
                // 3.2. ServiceBusReceiverClient obtained, consume messages then close ServiceBusReceiverClient
                final IterableStream<ServiceBusReceivedMessage> stream = receiverClient.receiveMessages(5, Duration.ofSeconds(5));
                final List<ServiceBusReceivedMessage> messages = stream.stream().collect(Collectors.toList());
                System.out.println("session:" + receiverClient.getSessionId() + " messages:" + messages.size());
            } catch (Exception t) {
                receiverErrorClosed = true;
                close("close(receiverClient) receive-error.", t, receiverClient);
            }
            if (!receiverErrorClosed) {
                close("close(receiverClient) receive-completed.", receiverClient);
            }

            // and ServiceBusSessionReceiverClient then repeat the cycle from step 2 on
            close("close(sessionClient) successful-cycle", sessionClient);
            sleep(250);
            i++;
        }

        waitKeyPress("Press To exit");
    }

    private static void close(String message, AutoCloseable c) {
        System.out.println(message);
        try {
            c.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void close(String message, Exception e, AutoCloseable c) {
        String error = e.getMessage();
        if (e.getCause() != null && e.getCause().getMessage() != null) {
            error = e.getCause().getMessage();
        }
        System.out.println(message + ": " + error);
        try {
            c.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private static void waitKeyPress() {
        Scanner input = new Scanner(System.in);
        input.nextLine();
    }

    private static void waitKeyPress(String message) {
        System.out.println("waitKeyPress... (" + message + ")");
        waitKeyPress();
    }
}
