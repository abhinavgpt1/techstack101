package workqueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

class Producer {
  private static final String QUEUE_NAME = "myrmq";

  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      boolean durability = true; // to let queue persist if RMQ restarts
      channel.queueDeclare(QUEUE_NAME, durability, false, false, null);
      for(int i=1; i<=30; i++){
        String message = "Msg: " + i;
        // persist msg in disk (works fine for simple queue)
        // basicProperties=null won't store the message even if queue is durable -> message losses from queue
        // moreover, if server restarts then, the inprocess unAck messages won't get re-queued leading to complete message loss
        channel.basicPublish("", QUEUE_NAME,  MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        Thread.sleep(1);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
