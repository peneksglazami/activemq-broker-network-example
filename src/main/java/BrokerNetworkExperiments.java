import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.Random;

/**
 * Пример отправки сообщений через сеть брокеров сообщений ActiveMQ.
 *
 * Перед запуском приложения необходимо запустить два docker-контейнера с ActiveMQ,
 * которые связаны через network connection. Для этого используйте docker/docker-compose.yml.
 *
 * @author Andrey Grigorov
 */
public class BrokerNetworkExperiments {

    private static final String MQ_01_ADDRESS = "tcp://localhost:61616";
    private static final String MQ_02_ADDRESS = "tcp://localhost:61617";
    private static final Properties PROPERTIES_1 = new Properties();
    private static final Properties PROPERTIES_2 = new Properties();

    static {
        PROPERTIES_1.put("java.naming.provider.url", "failover:(" + MQ_01_ADDRESS + "," + MQ_02_ADDRESS + ")?randomize=false&priorityBackup=true&jms.prefetchPolicy.all=1");
        PROPERTIES_1.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");

        PROPERTIES_2.put("java.naming.provider.url", "failover:(" + MQ_02_ADDRESS + "," + MQ_01_ADDRESS + ")?randomize=false&priorityBackup=true&jms.prefetchPolicy.all=1");
        PROPERTIES_2.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
    }

    public static void main(String[] args) throws Exception {
        // обычное поведение
        regular();

        // один consumer
        oneConsumer();

        // два consumer'а, но один медленный
        twoConsumerByOneIsSlow();

        // один producer
        oneProducer();

        // один producer, два consumer'а, но один медленный
        oneProducerTwoConsumerByOneIsSlow();

        // один producer и два consumer на разных брокерах; общение через топик
        oneSubscriberOnePublisher();
    }

    /**
     * Запущены 2 Active MQ. К каждому подключён 1 consumer. К каждому подключается по одному producer, которые
     * отправляют по 10 сообщений.
     * Сообщения от producer'а обрабатывает consumer, подключенный к тому же экземпляру брокера.
     *
     * @throws Exception
     */
    private static void regular() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 2 Producers, 2 Consumers");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/test");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/test");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1);
        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queueCluster2);
        consumer2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 2 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();
        connection2.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "DataCenter #1 - " + String.valueOf(i));
        }

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory2, queueCluster2, "DataCenter #2 - " + String.valueOf(i));
        }

        Thread.sleep(3000);
        System.out.println();

        session1.close();
        connection1.close();
        session2.close();
        connection2.close();
    }

    /**
     * Запущены 2 Active MQ. К одному подключён consumer. К каждому подключается по одному producer, которые
     * отправляют по 10 сообщений.
     * Consumer обрабатывает все запросы.
     *
     * @throws Exception
     */
    private static void oneConsumer() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 2 Producers, 1 Consumer (in DataCenter #1)");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/test");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/test");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1);
        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "DataCenter #1 - " + String.valueOf(i));
        }

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory2, queueCluster2, "DataCenter #2 - " + String.valueOf(i));
        }

        Thread.sleep(3000);

        System.out.println();

        session1.close();
        connection1.close();
    }


    /**
     * Запущены 2 Active MQ. К каждому подключёно по одному consumer. К каждому подключается по одному producer, которые
     * отправляют по 10 сообщений.
     * Один consumer медленный (каждое сообщение обрабатывает 1 секунду), другой быстрый.
     * Медленный consumer забирает одно сообщение от producer'а, подлючённого к тому же экземпляру брокера. Все
     * остальный сообщения от этого producer'а передаются на другой брокер.
     *
     * @throws Exception
     */
    private static void twoConsumerByOneIsSlow() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 2 Producers, 2 Consumers (1 slow - in DataCenter #2)");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/test");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/test");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1);
        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queueCluster2);
        consumer2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Consumer 2 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();
        connection2.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "DataCenter #1 - " + String.valueOf(i));
        }

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory2, queueCluster2, "DataCenter #2 - " + String.valueOf(i));
        }

        Thread.sleep(5000);
        System.out.println();

        session1.close();
        connection1.close();
        session2.close();
        connection2.close();
    }

    /**
     * Запущены 2 Active MQ. К каждому подключён 1 consumer. К одному ActiveMQ подключен один producer, который
     * отправляет 10 сообщений.
     * Все сообщения обрабатывает consumer, подключенный к тому же экземпляру брокера, что и producer.
     *
     * @throws Exception
     */
    private static void oneProducer() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 1 Producer (in DataCenter #1), 2 Consumers");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/test");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/test");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1);
        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queueCluster2);
        consumer2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 2 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();
        connection2.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "DataCenter #1 - " + String.valueOf(i));
        }

        Thread.sleep(3000);
        System.out.println();

        session1.close();
        connection1.close();
        session2.close();
        connection2.close();
    }

    /**
     * Запущены 2 Active MQ. К каждому подключён 1 consumer. К одному ActiveMQ подключен один producer, который
     * отправляет 10 сообщений. Consumer, подключённый к тому же ActiveMQ, что и producer, обрабатывает сообщения
     * медленно (2 секунды на сообщение).
     * Медленный consumer заберёт в обработку одно сообщение, всё остальные будут переданы другому брокеру.
     *
     * @throws Exception
     */
    private static void oneProducerTwoConsumerByOneIsSlow() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 1 Producer (in DataCenter #1), 2 Consumers (1 slow - in DataCenter #1)");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Queue queueCluster1 = (Queue) context1.lookup("dynamicQueues/test");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Queue queueCluster2 = (Queue) context2.lookup("dynamicQueues/test");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(queueCluster1);
        consumer1.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    Thread.sleep(2000);
                    System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(queueCluster2);
        consumer2.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    System.out.println("Consumer 2 processed message \"" + message.getJMSCorrelationID() + "\"");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection1.start();
        connection2.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory1, queueCluster1, "DataCenter #1 - " + String.valueOf(i));
        }

        Thread.sleep(3000);
        System.out.println();

        session1.close();
        connection1.close();
        session2.close();
        connection2.close();
    }

    private static void sendMessage(ConnectionFactory connectionFactory, Destination destination, String correlationId) throws JMSException {
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


    /**
     * Запущены 2 Active MQ. К каждому подключено по 1 consumer. К одному подключен producer, который
     * отправляют 10 сообщений в топик.
     * Consumer'ы на каждом брокере получают все запросы.
     *
     * @throws Exception
     */
    private static void oneSubscriberOnePublisher() throws Exception {
        System.out.println();
        System.out.println("2 Active MQ, 1 Producers (in DataCenter #2), 2 Consumer (in DataCenter #1 & #2), 1 topic");

        InitialContext context1 = new InitialContext(PROPERTIES_1);
        final ConnectionFactory connectionFactory1 = (ConnectionFactory) context1.lookup("ConnectionFactory");
        final Topic topicCluster1 = (Topic) context1.lookup("dynamicTopics/topic");

        InitialContext context2 = new InitialContext(PROPERTIES_2);
        final ConnectionFactory connectionFactory2 = (ConnectionFactory) context2.lookup("ConnectionFactory");
        final Topic topicCluster2 = (Topic) context2.lookup("dynamicTopics/topic");

        Connection connection1 = connectionFactory1.createConnection();
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer1 = session1.createConsumer(topicCluster1);
        consumer1.setMessageListener(message -> {
            try {
                System.out.println("Consumer 1 processed message \"" + message.getJMSCorrelationID() + "\"");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        Connection connection2 = connectionFactory2.createConnection();
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createConsumer(topicCluster2);
        consumer2.setMessageListener(message -> {
            try {
                System.out.println("Consumer 2 processed message \"" + message.getJMSCorrelationID() + "\"");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        connection1.start();
        connection2.start();

        for (int i = 1; i <= 10; i++) {
            sendMessage(connectionFactory2, topicCluster2, "DataCenter #2 - " + i);
        }

        Thread.sleep(3000);

        System.out.println();

        session1.close();
        connection1.close();
    }
}

