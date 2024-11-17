package routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// single queue should have single consumer. Single queue can have multiple consumers
// direct exchange - helps in selectively receiving the logs
public class WarnQueue2Receiver {
    private static String EXCHANGE_NAME = "exchange_direct";
    private static String WARN_QUEUE2 = "queue_warn_fanout2";
    public static void main(String args[]) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // same infra as in producer [WRONG - producer has no knowledge of queues]
        boolean durability = false;
        channel.exchangeDeclare(EXCHANGE_NAME, "direct", durability); // direct type exchange
        channel.queueDeclare(WARN_QUEUE2, durability, false, false, null);
        channel.queueBind(WARN_QUEUE2, EXCHANGE_NAME, Severity.WARN.name());

        System.out.println("[*] Waiting for msgs in " + WARN_QUEUE2 + "...");
        // deliverCallBack
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("[x] Received msg in " + WARN_QUEUE2 + ": " + message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); // this is important for msg to not be queued again
        };
        boolean autoAck = false;
        channel.basicConsume(WARN_QUEUE2, autoAck, deliverCallback, consumerTag->{});
    }
}
