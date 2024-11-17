package rpc;

// exclusive queue - queue which dispatches msgs to exclusively 1 consumer
// autoDelete queue - queue that has had at least one consumer is deleted when last consumer unsubscribes
// no need of basicQos(1) unless you want to load balance -> make sure to set autoAck=false and ack every msg once processed.
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RPCServer {
    private static final String RPC_QUEUE = "rpc_queue";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE, false, false, false, null);
        System.out.println("RPC server is listening...");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String message = new String(delivery.getBody(), "UTF-8");
            String response = String.format("Processed %s", message);
            try {
                Thread.sleep(Integer.parseInt(message) % 2 == 0 ? 1000: 2000);
                System.out.println("[x] Processed msg: " + message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                System.out.println("[x] Replied to this request with response: " + response);
            }
        };
        channel.basicConsume(RPC_QUEUE, true, deliverCallback, consumerTag->{});
    }
}
