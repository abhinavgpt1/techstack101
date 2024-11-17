package exchange2exchange;

import com.rabbitmq.client.*;

// receives messages to turn ON/OFF appliances at home
public class HomeInstructionReceiver {

    private static final String EXCHANGE_NAME = "exchange_direct_e2e";
    private static final String EXCHANGE_NAME_HOME = "exchange_direct_e2e_home";
    private static final String HOME_APPLIANCE_QUEUE = "home_appliance_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection(); // because of open connection/subscription, consumer never dies
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC); //can use DIRECT too for Exchange-Exchange connection
        channel.exchangeDeclare(EXCHANGE_NAME_HOME, BuiltinExchangeType.TOPIC); //can use DIRECT too for Exchange-Exchange connection
        channel.queueDeclare(HOME_APPLIANCE_QUEUE, false, false, false, null);
        channel.queueBind(HOME_APPLIANCE_QUEUE, EXCHANGE_NAME_HOME, "#.home.appliance.*");

        channel.exchangeBind(EXCHANGE_NAME_HOME, EXCHANGE_NAME, "home.appliance.#");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };

        channel.basicConsume(HOME_APPLIANCE_QUEUE, true, deliverCallback, System.out::println);
    }
}
