package routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// Producer, which emits logs like ERROR, INFO, WARN into Direct Exchange

public class EmitSeverityLogs {
    private static String EXCHANGE_NAME = "exchange_direct";

    public static void main(String args[]) {
        // use cases:
        // multiple bindings -> one consumer having multiple bindings with same exchange
        // fanout like binding -> 2 or more consumers binded with same bindingKey gets the same message
        // message with non-matching bindingKey -> should be dropped

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            boolean durability = false;
            channel.exchangeDeclare(EXCHANGE_NAME, "direct", durability); // direct type exchange

            // no queue info is available to producer

            // send messages
            channel.basicPublish(EXCHANGE_NAME, Severity.ERROR.name(), null, "Error: loreum ipsum error1".getBytes());
            channel.basicPublish(EXCHANGE_NAME, Severity.INFO.name(), null, "Info: Processed msgs".getBytes());
            channel.basicPublish(EXCHANGE_NAME, Severity.WARN.name(), null, "WARN: library not found".getBytes());
            channel.basicPublish(EXCHANGE_NAME, Severity.WARN.name(), null, "WARN: connection limit approaching".getBytes());

            channel.basicPublish(EXCHANGE_NAME, "non-linkedRoutingKey", null, "can anyone pick this message, otherwise it'll be dropped!!".getBytes());

            // above ERROR and INFO msg goes in 1 queue. WARN msgs goes in 3 queues - ALL_LOGS_QUEUE, WARN_QUEUE1 and WARN_QUEUE2
            System.out.println("[x] Sent all severity messages.");

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}