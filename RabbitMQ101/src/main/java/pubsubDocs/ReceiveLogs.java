package pubsubDocs;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

// launch 2 or more instances of consumer. all consumers will get the same message from producer.
public class ReceiveLogs {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue(); //creates random named queue - which is non-durable, exclusive and autodeletes once all consumers are done consuming it
        channel.queueBind(queueName, EXCHANGE_NAME, ""); //no need of routingKey in case of fanout exchange
        // NOTE: The messages will be lost if no queue is bound to the exchange yet
        // Although bindingKey is ignored if mentioned, but it can't be NULL ever

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}