package exchange2exchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

// produces messages to switch on/off appliances at various places
class InstructionProducer {
  private static final String EXCHANGE_NAME = "exchange_direct_e2e";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC); //can use DIRECT too for Exchange-Exchange connection

      channel.basicPublish(EXCHANGE_NAME, "home.appliance.on", null, "Turn ON appliances in home".getBytes());
      System.out.println(" [x] Sent message 1");
      channel.basicPublish(EXCHANGE_NAME, "office.appliance.off", null, "Turn OFF appliances in office".getBytes());
      System.out.println(" [x] Sent message 2");
      channel.basicPublish(EXCHANGE_NAME, "shop.appliance.off", null, "Turn OFF appliances in shop".getBytes());
      System.out.println(" [x] Sent message 3");
    }
  }
}
