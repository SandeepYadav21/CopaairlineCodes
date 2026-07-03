package solaceJava;

import com.solacesystems.jcsmp.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SolacePubishMsgWithHeaders - Fixed version with singleton JCSMP session.
 *
 * PROBLEM (original code):
 *   Same as SolacePublisher — every call created a new JCSMPSession.
 *   This class is called from acifeed.bwp's PublishCNSJSON activity
 *   to forward ACI feed messages with JMS-like dynamic properties
 *   as Solace user properties.
 *
 * FIX:
 *   - Singleton session pattern identical to SolacePublisher.
 *   - Parses the XML properties string from BW6 DynamicProperties
 *     and sets them as SDT user properties on the Solace message.
 *
 * NOTE: Class name kept as "SolacePubishMsgWithHeaders" (with typo)
 *       to match the existing BW6 Java Invoke configuration.
 */
public class SolacePubishMsgWithHeaders {

    private static JCSMPSession session = null;
    private static XMLMessageProducer producer = null;
    private static String currentSessionKey = null;
    private static final ReentrantLock lock = new ReentrantLock();

    /**
     * Sends a text message with dynamic properties to a Solace topic.
     * Reuses the JCSMP session across invocations.
     *
     * Method signature is kept identical to the original.
     *
     * @throws JCSMPException on Solace communication errors
     */
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
            String xmlPropertiesString) throws JCSMPException {

        lock.lock();
        try {
            ensureConnected(host, username, password, vpnName,
                    sslValidateCertificate, sslValidateCertificateDate,
                    sslTrustStore, sslTrustStorePassword);

            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
            TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
            msg.setText(messageText);
            msg.setDeliveryMode(DeliveryMode.DIRECT);

            // Parse and apply dynamic properties from BW6 XML
            if (xmlPropertiesString != null && !xmlPropertiesString.trim().isEmpty()) {
                applyDynamicProperties(msg, xmlPropertiesString);
            }

            producer.send(msg, topic);

        } catch (JCSMPException e) {
            closeSession();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Parses the BW6 DynamicProperties XML and sets them as
     * Solace SDT user properties on the message.
     *
     * Expected XML format from tib:render-xml($ReceiveACIFeed/DynamicProperties):
     * <DynamicProperties>
     *   <property>
     *     <name>PropertyName</name>
     *     <value>PropertyValue</value>
     *   </property>
     *   ...
     * </DynamicProperties>
     */
    private static void applyDynamicProperties(TextMessage msg, String xmlPropertiesString) {
        try {
            // Wrap in root element if not already wrapped
            String xml = xmlPropertiesString.trim();
            if (!xml.startsWith("<")) {
                return; // Not valid XML, skip
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

            SDTMap userProperties = JCSMPFactory.onlyInstance().createMap();

            NodeList properties = doc.getElementsByTagName("property");
            for (int i = 0; i < properties.getLength(); i++) {
                Element prop = (Element) properties.item(i);
                NodeList nameNodes = prop.getElementsByTagName("name");
                NodeList valueNodes = prop.getElementsByTagName("value");

                if (nameNodes.getLength() > 0 && valueNodes.getLength() > 0) {
                    String name = nameNodes.item(0).getTextContent();
                    String value = valueNodes.item(0).getTextContent();
                    if (name != null && !name.isEmpty()) {
                        userProperties.putString(name, value != null ? value : "");
                    }
                }
            }

            if (!userProperties.isEmpty()) {
                msg.setProperties(userProperties);
            }

        } catch (Exception e) {
            // Log but don't fail the publish — properties are supplementary
            System.err.println("[SolacePubishMsgWithHeaders] Warning: Could not parse dynamic properties: "
                    + e.getMessage());
        }
    }

    /**
     * Ensures a valid JCSMP session and producer exist.
     */
    private static void ensureConnected(
            String host, String username, String password, String vpnName,
            boolean sslValidateCertificate, boolean sslValidateCertificateDate,
            String sslTrustStore, String sslTrustStorePassword) throws JCSMPException {

        String sessionKey = buildSessionKey(host, username, vpnName);

        if (session != null && !session.isClosed() && sessionKey.equals(currentSessionKey)) {
            return;
        }

        closeSession();

        JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, host);
        properties.setProperty(JCSMPProperties.USERNAME, username);
        properties.setProperty(JCSMPProperties.PASSWORD, password);
        properties.setProperty(JCSMPProperties.VPN_NAME, vpnName);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, sslValidateCertificate);
        properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, sslValidateCertificateDate);

        if (sslTrustStore != null && !sslTrustStore.isEmpty()) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, sslTrustStore);
        }
        if (sslTrustStorePassword != null && !sslTrustStorePassword.isEmpty()) {
            properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, sslTrustStorePassword);
        }

        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);

        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object correlationKey) {
            }

            @Override
            public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
                System.err.println("[SolacePubishMsgWithHeaders] Producer error: " + cause.getMessage());
            }
        });

        currentSessionKey = sessionKey;
        System.out.println("[SolacePubishMsgWithHeaders] New JCSMP session established to "
                + host + " vpn=" + vpnName);
    }

    private static String buildSessionKey(String host, String username, String vpnName) {
        return host + "|" + username + "|" + vpnName;
    }

    private static void closeSession() {
        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) {}
            producer = null;
        }
        if (session != null) {
            try { session.closeSession(); } catch (Exception ignored) {}
            session = null;
        }
        currentSessionKey = null;
    }

    public static void cleanup() {
        lock.lock();
        try {
            closeSession();
            System.out.println("[SolacePubishMsgWithHeaders] Session cleaned up.");
        } finally {
            lock.unlock();
        }
    }
}