package headersExchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

class EmitLogs {
  private static final String EXCHANGE_NAME = "exchange_headers";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      channel.exchangeDeclare(EXCHANGE_NAME, "headers"); // use BuiltinExchangeType.HEADERS
      // message1 headers
      Map<String, Object> msg1Headers = new HashMap<>();
      msg1Headers.put(Severity.DEBUG.name(), "debug-header");
      msg1Headers.put(Severity.ERROR.name(), "error-header"); //header should match completely (key and value)
      AMQP.BasicProperties basicProperties1 = new AMQP.BasicProperties().builder().headers(msg1Headers).build();
      channel.basicPublish(EXCHANGE_NAME, "", basicProperties1, "message 1".getBytes()); //no routingKey, just BasicProperties

      System.out.println(" [x] Sent message 1");

      // message2 headers
      Map<String, Object> msg2Headers = new HashMap<>();
      msg2Headers.put(BugLevel.SEV1.name(), "sev1-header");
      msg2Headers.put(Severity.ERROR.name(), "error-header"); //header should match completely (key and value)
      AMQP.BasicProperties basicProperties2 = new AMQP.BasicProperties().builder().headers(msg2Headers).build();
      channel.basicPublish(EXCHANGE_NAME, "", basicProperties2, "message 2".getBytes()); //no routingKey, just BasicProperties

      System.out.println(" [x] Sent message 2");

      // message3 headers - will be lost due to no queue binding
      Map<String, Object> msg3Headers = new HashMap<>();
      msg3Headers.put(BugLevel.SEV3.name(), "sev3-header");
      AMQP.BasicProperties basicProperties3 = new AMQP.BasicProperties().builder().headers(msg3Headers).build();
      channel.basicPublish(EXCHANGE_NAME, "", basicProperties3, "message 3".getBytes()); //no routingKey, just BasicProperties

      System.out.println(" [x] Sent message 3");

      // message4 headers
      Map<String, Object> msg4Headers = new HashMap<>();
      msg4Headers.put(BugLevel.SEV4.name(), "sev4-header");
      AMQP.BasicProperties basicProperties4 = new AMQP.BasicProperties().builder().headers(msg4Headers).build();
      channel.basicPublish(EXCHANGE_NAME, "", basicProperties4, "message 4".getBytes()); //no routingKey, just BasicProperties

      System.out.println(" [x] Sent message 4");

    }
  }
}
