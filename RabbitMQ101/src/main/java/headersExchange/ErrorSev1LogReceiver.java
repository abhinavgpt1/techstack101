package headersExchange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

// receives all messages which are Severity.ERROR and SEV1 bugs
public class ErrorSev1LogReceiver {

    private static final String EXCHANGE_NAME = "exchange_headers";
    private static final String ERROR_SEV1_LOG_QUEUE = "error-sev1-log-queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection(); // because of open connection/subscription, consumer never dies
        Channel channel = connection.createChannel();

        // queue headers
        Map<String, Object> queueHeaders = new HashMap<>();
        queueHeaders.put("x-match", "all"); //Match any of the header
        queueHeaders.put(Severity.ERROR.name(), "error-header");
        queueHeaders.put(BugLevel.SEV1.name(), "sev1-header");

        channel.exchangeDeclare(EXCHANGE_NAME, "headers"); // use BuiltinExchangeType.HEADERS
        channel.queueDeclare(ERROR_SEV1_LOG_QUEUE, false, false, false, null);
        channel.queueBind(ERROR_SEV1_LOG_QUEUE, EXCHANGE_NAME, "", queueHeaders); // create binding btw queue & exchange. More than one binding is also possible
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            System.out.println(delivery.getEnvelope());
        };

        channel.basicConsume(ERROR_SEV1_LOG_QUEUE, true, deliverCallback, System.out::println);
    }
}
