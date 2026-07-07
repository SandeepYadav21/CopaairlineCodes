package solaceJava;
import com.solacesystems.jcsmp.*;
import java.util.logging.*;

public class SolacePublisher_backup {

    public static String sendMessageToSolace(String host, String username, String password, String vpnName, boolean sslValidateCertificate, boolean sslValidateCertificateDate, String sslTrustStore, String sslTrustStorePassword,String inputMessage,String topicName) throws JCSMPException {
        // Disable INFO logging
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        if (handlers[0] instanceof ConsoleHandler) {
            handlers[0].setLevel(Level.WARNING);
        }
        
        try {
        // used module properties instead of actual values for connection properties
        
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

        final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);

        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        msg.setText(inputMessage);

        session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object key) {
                System.out.println("Producer received response for msg: " + key);
            }
            @Override
            public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
                System.out.printf("Producer received error for msg: %s@%s - %s%n", key, timestamp, cause);
            }
            @Override
            public void responseReceived(String messageID) {
            	System.out.println("Message sent successfully. Message ID: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
            }
        }).send(msg, topic);

        //System.out.println("Message sent. Exiting.");
        
        // Close the session
        session.closeSession();
        
        return "SUCCESS";

        } catch (JCSMPException e) {
            e.printStackTrace();
            // Return null in case of error
            return null;
        }
}
}
