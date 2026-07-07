package solaceJava;
import com.solacesystems.jcsmp.*;


public class SolaceConsumTopicWithSelector {

	 public static String readMsgFromTopicWithSelctor(
	            String host,
	            String username,
	            String password,
	            String vpnName,
	            boolean sslValidateCertificate,
	            boolean sslValidateCertificateDate,
	            String sslTrustStore,
	            String sslTrustStorePassword,
	            String topicPattarn,
	            String compareID,
	            String QueueName,
	            int timeOutInMilliSec
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
	        System.out.println("Setting the selector as: '"+"COMPARE_ID = "+"'"+compareID+"''" );
	        flowProps.setSelector("COMPARE_ID = "+"'"+compareID+"'" );
	        final FlowReceiver consumer = session.createFlow(null, flowProps);

	        final StringBuilder receivedMessageHolder = new StringBuilder();
	        final boolean[] messageFound = {false}; // Flag to indicate message matching topic is found

	        try {
	            consumer.start();

	            System.out.println("Listening for messages on queue with topic filter: " + topicPattarn);

	            long startTime = System.currentTimeMillis();
	            while ((System.currentTimeMillis() - startTime) < timeOutInMilliSec) { // 30-second timeout
    try {
	                BytesXMLMessage message = consumer.receive(1000); // Poll for a message
	                System.out.println("**** Waiting for message ....");
	                if (message != null) {
	                    // Filter messages based on their destination topic
	                    String messageTopic = message.getDestination().getName();
	                    if (messageTopic.equals(topicPattarn)) {
	                        if (message instanceof TextMessage) {
	                        	//System.out.println("******2*********");
	                            String receivedMessage = ((TextMessage) message).getText();
	                            //System.out.println("******Message :"+receivedMessage);
	                            //String actualDest = message.getDestination().getName();
	                            //System.out.println("******3*********");
	                            String jmsDest="";
								//try {
									//jmsDest = message.getProperties().toString();
									/*SDTMap prop = message.getProperties();
									if (prop != null) {
									    System.out.println("---- Message Properties ----");
									    for (String key : prop.keySet()) {
									        try {
									            Object value = prop.get(key);
									            System.out.println(key + " = " + value);
									        } catch (SDTException e) {
									            System.out.println("Failed to read property: " + key);
									            e.printStackTrace();
									        }
									    }
									    System.out.println("----------------------------");
									} else {
									    System.out.println("No properties found in message.");
									}*/

									//System.out.println("******4*********");
											//.getString("JMSDestination");
								//} catch (SDTException e1) {
									//System.out.println("*** Error while getting the property : JMSDestination***");
									// TODO Auto-generated catch block
									//e1.printStackTrace();
								//}
	                            System.out.println("[DEBUG] JMSDestination: " + jmsDest);
	                            //System.out.println("[DEBUG] Actual Destination: " + actualDest);
	                            //System.out.println("[DEBUG] Message Content: " + receivedMessage);

	                            //System.out.println("Received matching message: " + receivedMessage);
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
    } catch (JCSMPException e) {
        System.err.println("Error receiving message: " + e.getMessage());
        break;
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
        
}
}