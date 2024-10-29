package org.example.sb;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main_Session_Sender {
    private static final String CON_STR = System.getenv("CON_STR");
    private static final String Q_NAME = System.getenv("Q_NAME");

    public static void main(String[] args) {
        final ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
                .connectionString(CON_STR);

        final ServiceBusSenderClient client = clientBuilder
                .sender()
                .queueName(Q_NAME)
                .buildClient();

        for (int i = 0; i < 6000; i++) {
            final ServiceBusMessage message = new ServiceBusMessage("hello");
            message.setSessionId(i + "");
            client.sendMessage(message);
            try {
                TimeUnit.MILLISECONDS.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        waitKeyPress("Done Sending");
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