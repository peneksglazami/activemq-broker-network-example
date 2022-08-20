import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Пример отправки сообщений через сеть брокеров сообщений ActiveMQ.
 * <p>
 * Перед запуском приложения необходимо запустить четыре docker-контейнера с ActiveMQ,
 * которые связаны через network connection. Для этого используйте docker/docker-compose.yml.
 *
 * @author Andrey Grigorov
 */
public class BrokerNetworkExperiments {

    private static final String MQ_01_ADDRESS = "tcp://localhost:61616";
    private static final String MQ_02_ADDRESS = "tcp://localhost:61617";
    private static final String MQ_03_ADDRESS = "tcp://localhost:61618";
    private static final String MQ_04_ADDRESS = "tcp://localhost:61619";
    private static final Properties PROPERTIES_1 = new Properties();
    private static final Properties PROPERTIES_2 = new Properties();
    private static final Properties PROPERTIES_3 = new Properties();
    private static final Properties PROPERTIES_4 = new Properties();

    static {
        PROPERTIES_1.put("java.naming.provider.url", "failover:(" + MQ_01_ADDRESS + "," + MQ_02_ADDRESS + "," + MQ_03_ADDRESS + "," + MQ_04_ADDRESS + ")?randomize=false&priorityBackup=true&priorityURIs=" + MQ_01_ADDRESS + "," + MQ_02_ADDRESS);
        PROPERTIES_1.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");

        PROPERTIES_2.put("java.naming.provider.url", "failover:(" + MQ_02_ADDRESS + "," + MQ_01_ADDRESS + "," + MQ_04_ADDRESS + "," + MQ_03_ADDRESS + ")?randomize=false&priorityBackup=true&priorityURIs=" + MQ_02_ADDRESS + "," + MQ_01_ADDRESS);
        PROPERTIES_2.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");

        PROPERTIES_3.put("java.naming.provider.url", "failover:(" + MQ_03_ADDRESS + "," + MQ_04_ADDRESS + "," + MQ_01_ADDRESS + "," + MQ_02_ADDRESS + ")?randomize=false&priorityBackup=true&priorityURIs=" + MQ_03_ADDRESS + "," + MQ_04_ADDRESS);
        PROPERTIES_3.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");

        PROPERTIES_4.put("java.naming.provider.url", "failover:(" + MQ_04_ADDRESS + "," + MQ_03_ADDRESS + "," + MQ_02_ADDRESS + "," + MQ_01_ADDRESS + ")?randomize=false&priorityBackup=true&priorityURIs=" + MQ_04_ADDRESS + "," + MQ_03_ADDRESS);
        PROPERTIES_4.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    }

    public static void main(String[] args) throws Exception {
        experiment();
    }

    /**
     * Запущены 4 Active MQ (dc1-amq01, dc1-amq02, dc2-amq03, dc2-amq04).
     * К каждому подключён по 1 consumer:
     * dc1-amq01, dc2-amq02 - слушатели очереди REQUEST с селектором service = 'A'
     * dc1-amq02, dc2-amq04 - слушатели очереди REQUEST с селектором service = 'B'
     * <p>
     * 1 producer подключён к dc1-amq01 и шлёт в очередь REQUEST 5 сообщений для сервиса A, после этого слушатель на
     * dc1-amq01 отключается, потом снова 5 сообщений в очередь REQUEST для сервиса A, затем 10 сообщений для сервиса B.
     *
     * @throws Exception при возникновении исключения
     */
    private static void experiment() throws Exception {
        System.out.println();
        System.out.println("2 DC, 4 Active MQ, 1 Producer, 4 Consumers");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/REQUEST");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/REQUEST");

        InitialContext context3 = new InitialContext(PROPERTIES_3);
        final ConnectionFactory connectionFactory3 = (ConnectionFactory) context3.lookup("ConnectionFactory");
        final Queue queueCluster3 = (Queue) context3.lookup("dynamicQueues/REQUEST");

        InitialContext context4 = new InitialContext(PROPERTIES_4);
        final ConnectionFactory connectionFactory4 = (ConnectionFactory) context4.lookup("ConnectionFactory");
        final Queue queueCluster4 = (Queue) context4.lookup("dynamicQueues/REQUEST");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1, "service = 'A'");
        consumer1.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer A (dc1-amq01) processed message \"" + message.getJMSCorrelationID() + "\"; service = " + message.getStringProperty("service"));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queueCluster2, "service = 'B'");
        consumer2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer B (dc1-amq02) processed message \"" + message.getJMSCorrelationID() + "\"; service = " + message.getStringProperty("service"));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection3 = connectionFactory3.createConnection();
        Session session3 = connection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createConsumer(queueCluster3, "service = 'A'");
        consumer3.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer A (dc2-amq03) processed message \"" + message.getJMSCorrelationID() + "\"; service = " + message.getStringProperty("service"));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection4 = connectionFactory4.createConnection();
        Session session4 = connection4.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer4 = session4.createConsumer(queueCluster4, "service = 'B'");
        consumer4.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer B (dc2-amq04) processed message \"" + message.getJMSCorrelationID() + "\"; service = " + message.getStringProperty("service"));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();
        connection2.start();
        connection3.start();
        connection4.start();

        Thread.sleep(1000);

        for (int i = 1; i <= 5; i++) {
            sendMessage(connectionFactory1, queueCluster1, "Message to A from dc1-amq01 - " + String.valueOf(i), Collections.singletonMap("service", "A"));
        }

        Thread.sleep(1000);
        consumer1.close();

        for (int i = 6; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "Message to A from dc1-amq01 - " + String.valueOf(i), Collections.singletonMap("service", "A"));
        }

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "Message to B from dc1-amq01 - " + String.valueOf(i), Collections.singletonMap("service", "B"));
        }

        Thread.sleep(30000);
        System.out.println();

        session1.close();
        connection1.close();
        session2.close();
        connection2.close();
        session3.close();
        connection3.close();
        session4.close();
        connection4.close();
    }

    private static void sendMessage(ConnectionFactory connectionFactory, Destination destination,
                                    String correlationId, Map<String, String> headers) throws JMSException {
        Connection connection = connectionFactory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            try {
                BytesMessage bytesMessage = session.createBytesMessage();

                byte[] bytes = new byte[10000];
                new Random().nextBytes(bytes);
                bytesMessage.writeBytes(bytes);
                bytesMessage.setJMSCorrelationID(correlationId);
                if (headers != null) {
                    headers.forEach((key, value) -> {
                        try {
                            bytesMessage.setStringProperty(key, value);
                        } catch (JMSException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                MessageProducer messageProducer = session.createProducer(destination);
                try {
                    messageProducer.setTimeToLive(30000);
                    messageProducer.send(bytesMessage);
                } finally {
                    messageProducer.close();
                }
                session.commit();
            } finally {
                session.close();
            }
        } finally {
            connection.close();
        }
    }
}

