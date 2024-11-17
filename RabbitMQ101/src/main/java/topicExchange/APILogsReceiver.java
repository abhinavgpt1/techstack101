package topicExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// single queue should have single consumer. Single queue can have multiple consumers
// topic exchange - helps in receiving msgs based on pattern
// allows all api.* routingKey msgs
public class APILogsReceiver {
    private static String EXCHANGE_NAME = "exchange_topic";
    private static String API_LOGS_QUEUE = "queue_api_logs_only";

    public static void main(String args[]) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // declaring & defining infra for exchange, queue + bindings
        boolean durability = false;
        channel.exchangeDeclare(EXCHANGE_NAME, "topic", durability); // topic type exchange
        channel.queueDeclare(API_LOGS_QUEUE, durability, false, false, null);
        channel.queueBind(API_LOGS_QUEUE, EXCHANGE_NAME, "api.*"); // receiving api logs which follow contract
        // api.# would include any msg which has "api" as prefix in the routingKey eg. api.93urdoejd, api.sdkfl.sjkdf, etc.
        channel.queueBind(API_LOGS_QUEUE, EXCHANGE_NAME, "directthistoapiqueue"); // receiving direct exchange like msg

        System.out.println("[*] Waiting for msgs in " + API_LOGS_QUEUE + "...");
        // deliverCallBack
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[x] Received msg in " + API_LOGS_QUEUE + ": " + message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // this is important for msg to not be queued again
        };
        boolean autoAck = false;
        channel.basicConsume(API_LOGS_QUEUE, autoAck, deliverCallback, consumerTag->{});
    }
}
