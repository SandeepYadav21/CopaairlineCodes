package Solace.java;
import com.solacesystems.jcsmp.*;

import java.util.concurrent.locks.ReentrantLock;

/**
 * SolacePublisher - Fixed version with singleton JCSMP session.
 *
 * PROBLEM (original code):
 *   Every call to sendMessageToSolace() created a new JCSMPSession,
 *   published one message, and either closed or leaked the session.
 *   When called inside a ForEach loop (e.g., acifeed.bwp iterating
 *   over N PNR records), this produced N simultaneous connections
 *   to the Solace broker per process execution.
 *
 * FIX:
 *   - Maintain a single static JCSMPSession that is lazily initialized
 *     and reused across all invocations.
 *   - Use a static XMLMessageProducer bound to that session.
 *   - Only reconnect if the session is closed or connection params change.
 *   - Thread-safe via ReentrantLock for BW6 concurrent job execution.
 *   - Includes a public cleanup() method for graceful shutdown.
 */
public class SolacePublisher {

    private static JCSMPSession session = null;
    private static XMLMessageProducer producer = null;
    private static String currentSessionKey = null;
    private static final ReentrantLock lock = new ReentrantLock();

    /**
     * Sends a text message to a Solace topic.
     * Reuses the JCSMP session across invocations.
     *
     * Method signature is kept identical to the original so that
     * the BW6 Java Invoke activities do not need XML changes.
     *
     * @return "OK" on success
     * @throws JCSMPException on Solace communication errors
     */
    public static String sendMessageToSolace(
            String host,
            String username,
            String password,
            String vpnName,
            boolean sslValidateCertificate,
            boolean sslValidateCertificateDate,
            String sslTrustStore,
            String sslTrustStorePassword,
            String inputMessage,
            String topicName) throws JCSMPException {

        lock.lock();
        try {
            ensureConnected(host, username, password, vpnName,
                    sslValidateCertificate, sslValidateCertificateDate,
                    sslTrustStore, sslTrustStorePassword);

            Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
            TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
            msg.setText(inputMessage);
            msg.setDeliveryMode(DeliveryMode.DIRECT);

            producer.send(msg, topic);

            return "SUCCEES";

        } catch (JCSMPException e) {
            // Session may be stale — force cleanup so next call reconnects
            closeSession();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Ensures a valid JCSMP session and producer exist.
     * Creates them only if missing or if connection parameters changed.
     */
    private static void ensureConnected(
            String host, String username, String password, String vpnName,
            boolean sslValidateCertificate, boolean sslValidateCertificateDate,
            String sslTrustStore, String sslTrustStorePassword) throws JCSMPException {

        String sessionKey = buildSessionKey(host, username, vpnName);

        // Reuse existing session if still valid and params haven't changed
        if (session != null && !session.isClosed() && sessionKey.equals(currentSessionKey)) {
            return;
        }

        // Close stale session if any
        closeSession();

        // Create new session
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

        // Reconnect settings for resilience
        properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);
        properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS, false);

        session = JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object correlationKey) {
                // Guaranteed message ACK — not used for DIRECT delivery
            }

            @Override
            public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
                System.err.println("[SolacePublisher] Producer error: " + cause.getMessage());
            }
        });

        currentSessionKey = sessionKey;
        System.out.println("[SolacePublisher] New JCSMP session established to " + host + " vpn=" + vpnName);
    }

    /**
     * Builds a key to detect if connection parameters have changed.
     */
    private static String buildSessionKey(String host, String username, String vpnName) {
        return host + "|" + username + "|" + vpnName;
    }

    /**
     * Safely closes the current session and producer.
     */
    private static void closeSession() {
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception ignored) {
            }
            producer = null;
        }
        if (session != null) {
            try {
                session.closeSession();
            } catch (Exception ignored) {
            }
            session = null;
        }
        currentSessionKey = null;
    }

    /**
     * Public cleanup method — call from a BW6 shutdown hook or
     * OSGi deactivate if needed.
     */
    public static void cleanup() {
        lock.lock();
        try {
            closeSession();
            System.out.println("[SolacePublisher] Session cleaned up.");
        } finally {
            lock.unlock();
        }
    }
}
