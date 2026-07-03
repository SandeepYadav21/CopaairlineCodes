package solaceJava;
import com.solacesystems.jcsmp.*;
import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;


public class SolacePubishMsgWithHeaders2_backup {



	public static void sendMessageWithProperties(
	        String host,
	        String username,
	        String password,
	        String vpnName,
	        boolean sslValidateCertificate,
	        boolean sslValidateCertificateDate,
	        String sslTrustStore,
	        String sslTrustStorePassword,
	        String topicName,
	        String messageText,
	        String xmlPropertiesString // Input XML string
	) throws JCSMPException {

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

	    final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
	        public void responseReceived(String messageID) {
	            System.out.println("Producer received response for msg: " + messageID);
	        }

	        public void handleError(String messageID, JCSMPException e, long timestamp) {
	            System.out.println("Producer received error for msg: " + messageID + " - " + e);
	        }
	    });

	    final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
	    TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
	    msg.setText(messageText);

	    if (xmlPropertiesString != null && !xmlPropertiesString.trim().isEmpty()) {
	        try {
	        	
	            SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();

	            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	            dbFactory.setNamespaceAware(true);  // handle xmlns
	            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	            Document doc = dBuilder.parse(new ByteArrayInputStream(xmlPropertiesString.getBytes(StandardCharsets.UTF_8)));
	            doc.getDocumentElement().normalize();

	            NodeList propertyNodes = doc.getElementsByTagNameNS("*", "property");

	            for (int i = 0; i < propertyNodes.getLength(); i++) {
	                Node node = propertyNodes.item(i);
	                if (node.getNodeType() == Node.ELEMENT_NODE) {
	                    Element propElem = (Element) node;
	                    String name = propElem.getElementsByTagNameNS("*", "name").item(0).getTextContent();
	                    String value = propElem.getElementsByTagNameNS("*", "value").item(0).getTextContent();
	                    sdtMap.putString(name, value); // Always treat as string
	                }
	            }

	            if (!sdtMap.isEmpty()) {
	                msg.setProperties(sdtMap);
	                System.out.println("Setting properties as: " + sdtMap);
	            }

	        } catch (Exception e) {
	            System.out.println("Failed to parse XML properties: " + e.getMessage());
	            e.printStackTrace();
	        }
	    }

	    System.out.println("Sending message to topic: " + topicName);
	    producer.send(msg, topic);
	    session.closeSession();
	}

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
