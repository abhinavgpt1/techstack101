package topicExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import routing.Severity;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// single queue should have single consumer. Single queue can have multiple consumers
// topic exchange - helps in receiving msgs based on pattern
// allows all msgs
public class AllMsgReceiver {
    private static String EXCHANGE_NAME = "exchange_topic";
    private static String LITERALLY_ALL_LOGS_QUEUE = "queue_literally_all_logs";

    public static void main(String args[]) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // declaring & defining infra for exchange, queue + bindings
        boolean durability = false;
        channel.exchangeDeclare(EXCHANGE_NAME, "topic", durability); // topic type exchange
        channel.queueDeclare(LITERALLY_ALL_LOGS_QUEUE, durability, false, false, null);
        channel.queueBind(LITERALLY_ALL_LOGS_QUEUE, EXCHANGE_NAME, "#"); // receives all msgs literally
        // routingKey as *.* means receives all msgs following our contract (<xyz>.<abc>)
        // having multiple routingKey valid for a msg doesn't allow the msg to be sent that many times.

        System.out.println("[*] Waiting for msgs in " + LITERALLY_ALL_LOGS_QUEUE + "...");
        // deliverCallBack
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[x] Received msg in " + LITERALLY_ALL_LOGS_QUEUE + ": " + message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // this is important for msg to not be queued again
        };
        boolean autoAck = false;
        channel.basicConsume(LITERALLY_ALL_LOGS_QUEUE, autoAck, deliverCallback, consumerTag->{});
    }
}
