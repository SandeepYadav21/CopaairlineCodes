package solaceJava;

import com.solacesystems.jcsmp.*;

public class SolaceTopicConsumer {
    public static String listenToTopic(
            String host,
            String username,
            String password,
            String vpnName,
            boolean sslValidateCertificate,
            boolean sslValidateCertificateDate,
            String sslTrustStore,
            String sslTrustStorePassword,
            String topicPattern
    ) throws JCSMPException {
        // Create a JCSMP session
        final JCSMPProperties properties = new JCSMPProperties();

        properties.setProperty(JCSMPProperties.HOST, host);
        properties.setProperty(JCSMPProperties.USERNAME, username);
        properties.setProperty(JCSMPProperties.PASSWORD, password);
        properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, sslValidateCertificate);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, sslValidateCertificateDate);
        properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, sslTrustStore);
        properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, sslTrustStorePassword);

        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // Create a topic using the provided pattern
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicPattern);

        // Add a subscription to the topic pattern
        session.addSubscription(topic);

        // Implement a message listener for asynchronous consumption
        final StringBuilder receivedMessageHolder = new StringBuilder(); // To store the received message
        final XMLMessageConsumer consumer = session.getMessageConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage message) {
                if (message instanceof TextMessage) {
                    String receivedMessage = ((TextMessage) message).getText();
                    System.out.println("Received message on topic: " + receivedMessage);
                    synchronized (receivedMessageHolder) {
                        receivedMessageHolder.append(receivedMessage);
                        receivedMessageHolder.notify(); // Notify the main thread
                    }
                }
                // Acknowledge the message
                message.ackMessage();
            }

            @Override
            public void onException(JCSMPException e) {
                System.err.println("Consumer received an exception: " + e.getMessage());
            }
        });

        // Start the consumer
        consumer.start();

        System.out.println("Listening to topic pattern: " + topicPattern);

        // Wait for a message to arrive
        synchronized (receivedMessageHolder) {
            try {
                receivedMessageHolder.wait(60000); // Wait for 1 minute
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Close the consumer and session
        consumer.close();
        session.closeSession();

        // Return the received message
        return receivedMessageHolder.toString();
    }

    public static String readMessageFromTopic(
            String host,
            String username,
            String password,
            String vpnName,
            boolean sslValidateCertificate,
            boolean sslValidateCertificateDate,
            String sslTrustStore,
            String sslTrustStorePassword,
            String topicPattarn,
            String QueueName
    ) throws JCSMPException {
        // Create a JCSMP session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, host);
        properties.setProperty(JCSMPProperties.USERNAME, username);
        properties.setProperty(JCSMPProperties.PASSWORD, password);
        properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, sslValidateCertificate);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, sslValidateCertificateDate);

        if (sslTrustStore != null) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, sslTrustStore);
        }
        if (sslTrustStorePassword != null) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, sslTrustStorePassword);
        }

        final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        // Access the queue
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(QueueName);

        // Create a consumer for the queue
        final ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(queue); // Bind to the queue
        flowProps.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        final FlowReceiver consumer = session.createFlow(null, flowProps);

        final StringBuilder receivedMessageHolder = new StringBuilder();
        final boolean[] messageFound = {false}; // Flag to indicate message matching topic is found

        try {
            consumer.start();

            System.out.println("Listening for messages on queue with topic filter: " + topicPattarn);

            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 30000) { // 30-second timeout
                BytesXMLMessage message = consumer.receive(1000); // Poll for a message
                if (message != null) {
                    // Filter messages based on their destination topic
                    String messageTopic = message.getDestination().getName();
                    if (messageTopic.equals(topicPattarn)) {
                        if (message instanceof TextMessage) {
                            String receivedMessage = ((TextMessage) message).getText();
                            System.out.println("Received matching message: " + receivedMessage);
                            synchronized (receivedMessageHolder) {
                                receivedMessageHolder.append(receivedMessage);
                                messageFound[0] = true;
                            }
                        }
                        message.ackMessage(); // Acknowledge only matching message
                        break;
                    } else {
                        System.out.println("Discarded message from topic: " + messageTopic);
                    }
                }
            }

            if (!messageFound[0]) {
                System.out.println("No matching message received within timeout.");
                return null;
            }

            return receivedMessageHolder.toString();
        } catch (JCSMPException e) {
            throw new JCSMPException("Error in Solace consumer: " + e.getMessage(), e);
        } finally {
            // Close the consumer and session to clean up resources
            consumer.close();
            session.closeSession();
        }
    }
    
    public static void main(String[] args) {
        try {
            String topicPattern = "copa/*/checkin/boarding/flight/initiated/v1.0/cm*/#";
            String message = listenToTopic(
                "tcp://localhost:55555",
                "username",
                "password",
                "vpnName",
                false,
                false,
                null,
                null,
                topicPattern
            );
            System.out.println("Final message received: " + message);
        } catch (JCSMPException e) {
            e.printStackTrace();
        }
    }
}
