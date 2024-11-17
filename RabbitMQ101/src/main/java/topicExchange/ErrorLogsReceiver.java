package topicExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// single queue should have single consumer. Single queue can have multiple consumers
// topic exchange - helps in receiving msgs based on pattern
// allows all *.error routingKey msgs
public class ErrorLogsReceiver {
    private static String EXCHANGE_NAME = "exchange_topic";
    private static String ERROR_LOGS_QUEUE = "queue_error_logs_only";

    public static void main(String args[]) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // declaring & defining infra for exchange, queue + bindings
        boolean durability = false;
        channel.exchangeDeclare(EXCHANGE_NAME, "topic", durability); // topic type exchange
        channel.queueDeclare(ERROR_LOGS_QUEUE, durability, false, false, null);
        channel.queueBind(ERROR_LOGS_QUEUE, EXCHANGE_NAME, "*.error"); // receiving error logs which follow contract
        // #.error would include any msg which has "error" as suffix in the routingKey
        // eg. error, .error, api.sdkfl.error, etc.
        // here error will be valid since #.error means zero or more words behind error

        System.out.println("[*] Waiting for msgs in " + ERROR_LOGS_QUEUE + "...");
        // deliverCallBack
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[x] Received msg in " + ERROR_LOGS_QUEUE + ": " + message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // this is important for msg to not be queued again
        };
        boolean autoAck = false;
        channel.basicConsume(ERROR_LOGS_QUEUE, autoAck, deliverCallback, consumerTag->{});
    }
}
