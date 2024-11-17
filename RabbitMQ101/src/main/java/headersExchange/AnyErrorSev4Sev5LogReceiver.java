package headersExchange;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

// receives all messages having either Severity.ERROR or is a SEV4 or SEV5 bug
public class AnyErrorSev4Sev5LogReceiver {

    private static final String EXCHANGE_NAME = "exchange_headers";
    private static final String ERROR_SEV4_SEV5_LOG_QUEUE = "error-sev4-sev5-log-queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection(); // because of open connection/subscription, consumer never dies
        Channel channel = connection.createChannel();

        // queue headers
        Map<String, Object> queueHeaders = new HashMap<>();
        queueHeaders.put("x-match", "any"); //Match any of the header
        queueHeaders.put(Severity.ERROR.name(), "error-header");
        queueHeaders.put(BugLevel.SEV4.name(), "sev4-header");
        queueHeaders.put(BugLevel.SEV5.name(), "sev5-header");

        channel.exchangeDeclare(EXCHANGE_NAME, "headers"); // use BuiltinExchangeType.HEADERS
        channel.queueDeclare(ERROR_SEV4_SEV5_LOG_QUEUE, false, false, false, null);
        channel.queueBind(ERROR_SEV4_SEV5_LOG_QUEUE, EXCHANGE_NAME, "", queueHeaders); // create binding btw queue & exchange. More than one binding is also possible
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            System.out.println(delivery.getEnvelope());
        };

        channel.basicConsume(ERROR_SEV4_SEV5_LOG_QUEUE, true, deliverCallback, System.out::println);
    }
}
