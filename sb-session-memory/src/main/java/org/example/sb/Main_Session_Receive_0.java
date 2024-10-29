package org.example.sb;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public final class Main_Session_Receive_0 {
    private static final String CON_STR = System.getenv("CON_STR");
    private static final String Q_NAME = System.getenv("Q_NAME");

    public static void main(String[] args) {
        // 1. Create a ServiceBusClientBuilder
        final ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
                .connectionString(CON_STR);

        int i = 0;
        while (i < 100) {
            System.out.println("Creating ServiceBusSessionReceiverClient #" + i);
            // 2. Create a ServiceBusSessionReceiverClient
            ServiceBusSessionReceiverClient sessionClient = clientBuilder
                    .sessionReceiver()
                    .queueName(Q_NAME)
                    .receiveMode(ServiceBusReceiveMode.PEEK_LOCK)
                    .disableAutoComplete()
                    .prefetchCount(0)
                    .buildClient();

            try {
                // 3. Try to obtain a session using ServiceBusSessionReceiverClient.acceptNextSession()
                final ServiceBusReceiverClient receiverClient = sessionClient.acceptNextSession();
            } catch (Exception e) {
                // 4. We get a timeout ("Timeout on blocking read for") with no active session/producer , call ServiceBusSessionReceiverClient.close()
                sessionClient.close();
            }
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // 5. Repeat the cycle from step 2 on
            i++;
        }
        waitKeyPress();
    }

    private static void waitKeyPress() {
        Scanner input = new Scanner(System.in);
        input.nextLine();
    }

}
