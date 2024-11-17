package rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCClient {
    private static final String RPC_QUEUE = "rpc_queue";

    public static void main(String[] args) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try(Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel()){
            final String correlationId = UUID.randomUUID().toString();
//            String callbackQueueName = channel.queueDeclare().getQueue(); // non-durable, exclusive, auto-delete queue
//            above queue won't let multiple msgs be sent since it gets destroyed after consumption is over
            String callbackQueueName = "callbackqueue";
             channel.queueDeclare(callbackQueueName, false, true, false, null); // exclusive queue which auto-deletes once all msgs are consumed

            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(correlationId)
                    .replyTo(callbackQueueName)
                    .build();

// channel.queueDeclare(RPC_QUEUE, false, false, false, null); // letting RPCServer create it. NOTE: if this queue doesn't exist when client produces, then msg will be dropped
            for(int i=1; i<=10; i++){
                channel.basicPublish("", RPC_QUEUE, props, String.valueOf(i).getBytes());
                System.out.println("[x] Sent message: " + i);
                System.out.println("Now waiting for callback of this msg...");

                final CompletableFuture<String> response = new CompletableFuture<>();

                String ctag = channel.basicConsume(callbackQueueName, true, (consumerTag, delivery) -> {
                    if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                        response.complete(new String(delivery.getBody(), "UTF-8"));
                    }
                }, consumerTag -> {});

                String result = response.get();
                channel.basicCancel(ctag);
                System.out.println("[x] Received this from callback for " + i + ": " + result);
            }
        }
    }
}
