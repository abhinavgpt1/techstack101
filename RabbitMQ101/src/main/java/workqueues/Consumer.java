package workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * FYI: RabbitMQ is push based i.e. consumers are sent message whereas in Kafka it is pull based i.e. with the help of polling the messages are checked & fetched by consumers.
 * Multiple consumers are run at the same time. Num of producers remains 1. Queue=1.
 * on stopping 1 consumer, the messages are requeued if autoAck=false.
 * They are consumed after default/custom timeout
 */
public class Consumer {

    private final static String QUEUE_NAME = "myrmq";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection(); // because of open connection/subscription, consumer never dies
        Channel channel = connection.createChannel();

        boolean durability = true; // to let queue persist if RMQ restarts
        channel.queueDeclare(QUEUE_NAME, durability, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        int prefetchCount = 1;
        channel.basicQos(prefetchCount); // RabbitMQ doesn't dispatch a new message to a worker until it has processed and acknowledged the previous one

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            try {
                Thread.sleep((message.length()%2)*1000);
                if(Integer.parseInt(message.substring(message.indexOf(':') + 2)) % 2 == 1)
                    Thread.sleep(1000); //adding extra time for odd msg ids to check channel.basicQos(1)

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        /*
            On average every consumer will get the same number of messages.
            This way of distributing messages is called round-robin.

            The consumer stopped won't have its message re-queued if autoAck=true

            When 2 consumers are launched -> all odd id msg are handled by one consumer, and rest by others.
            this is because of fair dispatch = This happens because RabbitMQ just dispatches a message when the message enters the queue. It doesn't look at the number of unacknowledged messages for a consumer. It just blindly dispatches every n-th message to the n-th consumer.
         */
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });
        // Q1 - what if basicQos(1) is set and no consumer is available to receive the message producer produces?
        // ans1 - queue will pile up leading to message loss -> which may be acceptable in some applications but not in others where message integrity is critical. (https://www.geeksforgeeks.org/what-happens-when-message-queue-is-full)
        // Q2 - can durable queue persist a message without storing to disk?
        // ans2 - no. check ans in Producer file
    }
}
