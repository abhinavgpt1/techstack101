package topicExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import routing.Severity;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// Producer, which emits logs like error, info, warn for entities like api, ui, aws into Topic Exchange with various dot delimited routing keys of the format <entity>.<severity>

public class EmitLogs {
    private static String EXCHANGE_NAME = "exchange_topic";

    public static void main(String args[]) {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            boolean durability = false;
            channel.exchangeDeclare(EXCHANGE_NAME, "topic", durability); // topic type exchange

            // no queue info is available to producer
            // if no valid queue exists at the time of message publish, the msg gets dropped. eg. I ran this file 1st and AllErrorsReceiver next -> the msgs were lost

            // not having consumer up != not having queue up
            // -> if queue is up during msg publish (and the routingKey=bindingKey), then the msg won't be lost unless server is restarted and msg persistence is OFF
            // -> if queue has the msg, the consumer would have the msg when it gets live.
            //
            // Recall, in the beginning we were declaring queue in both producer and consumer code

            // send messages
            channel.basicPublish(EXCHANGE_NAME, "api.error", null, "api: error: loreum ipsum".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "api.warn", null, "api: warn: loreum ipsum".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "api.info", null, "api: info: loreum ipsum".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "ui.error", null, "ui: error: loreum ipsum".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "aws.error", null, "aws: error: loreum ipsum".getBytes());

            // routing key contract breach
            channel.basicPublish(EXCHANGE_NAME, "aws.ecs.info", null, "this msg is picked if routingKey has #".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "directthistoapiqueue", null, "When special characters, * (star) and # (hash), aren't used in bindings, the topic exchange will behave just like a direct one.".getBytes());
            channel.basicPublish(EXCHANGE_NAME, "needsmiracleforthis", null, "pick me please".getBytes());

            System.out.println("[x] Sent all <entity>.<severity> messages.");

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}