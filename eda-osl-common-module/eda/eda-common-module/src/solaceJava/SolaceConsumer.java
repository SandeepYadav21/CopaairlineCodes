package solaceJava;

import com.solacesystems.jcsmp.*;

public class SolaceConsumer {
    public static String consumeMessageSolace(String host, String username, String password, String vpnName, boolean sslValidateCertificate, boolean sslValidateCertificateDate, String sslTrustStore, String sslTrustStorePassword, String queueName) throws JCSMPException {
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
        // Create a queue
        //Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        
        // Provision the queue (ensure it exists)
        //session.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);

        // Create a consumer flow for the queue
        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(queue);
        flowProps.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

        // Create a flow receiver
        FlowReceiver flowReceiver = session.createFlow(null, flowProps);
        
        // Start the consumer
        flowReceiver.start();

        // Receive a message
        BytesXMLMessage message = flowReceiver.receive(1000); // Wait for up to _/1000 seconds

        // Process the received message
        String consumedMessage = null;
        if (message != null) {
            if (message instanceof TextMessage) {
                consumedMessage = ((TextMessage) message).getText();
                System.out.println("Received message: " + consumedMessage);
            }
            // Acknowledge the message
            message.ackMessage();
        } else {
            System.out.println("No message received within the timeout period.");
        }

        // Close the receiver and session
        flowReceiver.close();
        session.closeSession();

        // Return the consumed message
        return consumedMessage;
    }
}
