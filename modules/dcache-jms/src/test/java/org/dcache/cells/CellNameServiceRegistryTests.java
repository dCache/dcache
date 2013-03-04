package org.dcache.cells;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Set of tests to verify the CellNameServiceRegistry
 */
public class CellNameServiceRegistryTests {

    public static final String DOMAIN_1 = "domain-1";
    public static final String[] DOMAIN_1_CELLS = { "d1-c1", "d1-c2", "d1-c3" };

    public static final String DOMAIN_2 = "domain-2";
    public static final String[] DOMAIN_2_CELLS = { "d2-c1", "d2-c2" };

    public static final String UNKNOWN_CELL = "unknown-cell";

    CellNameServiceRegistry _registry;

    @Before
    public void setup() {
        _registry = new CellNameServiceRegistry();
        simulateDomainRegistration(DOMAIN_1, 100, DOMAIN_1_CELLS);
        simulateDomainRegistration(DOMAIN_2, 100, DOMAIN_2_CELLS);
    }

    @Test
    public void testGetDomains() {
        Set<String> foundDomains = new HashSet<>(_registry.getDomains());
        Set<String> expectedDomains = Sets.newHashSet(DOMAIN_1, DOMAIN_2);
        assertEquals(expectedDomains, foundDomains);
    }

    @Test
    public void testGetDomainForKnownCells() {
        for( String cell : DOMAIN_1_CELLS) {
            String domain = _registry.getDomain(cell);
            assertEquals(DOMAIN_1, domain);
        }

        for( String cell : DOMAIN_2_CELLS) {
            String domain = _registry.getDomain(cell);
            assertEquals(DOMAIN_2, domain);
        }
    }

    @Test
    public void testGetDomainForUnknownCells() {
        String domain = _registry.getDomain(UNKNOWN_CELL);
        assertNull(domain);
    }

    @Test
    public void testExpiredDomain() throws InterruptedException {
        String expiringDomain = "new-domain";
        String expiringDomainsCell = "well-known";
        simulateDomainRegistration(expiringDomain, 1, expiringDomainsCell);

        Thread.sleep(2);

        assertNull(_registry.getDomain(expiringDomainsCell));

        Set<String> domains = new HashSet<>(_registry.getDomains());
        Set<String> expectedDomains = Sets.newHashSet(DOMAIN_1, DOMAIN_2);
        assertEquals(expectedDomains, domains);
    }

    @Test
    public void testDomainUpdatesRunningNewCell() throws InterruptedException {
        String newCell = "d2-c3";
        Set<String> cells = Sets.newHashSet(DOMAIN_2_CELLS);
        cells.add(newCell);

        simulateDomainRegistration(DOMAIN_2, 100, newCell);

        assertEquals(DOMAIN_2, _registry.getDomain( newCell));
    }

    @Test
    public void testDomainUpdatesWithOneLessCell() throws InterruptedException {
        String deadCell = DOMAIN_2_CELLS[0];
        Set<String> cells = Sets.newHashSet(DOMAIN_2_CELLS);
        cells.remove(deadCell);

        simulateDomainRegistration(DOMAIN_2, 100, cells);

        assertNull(_registry.getDomain( deadCell));
    }

    @Test
    public void testDomainUnregistersItself() throws InterruptedException {
        simulateDomainRegistration(DOMAIN_1, 0);

        for( String cell : DOMAIN_1_CELLS) {
            assertNull(_registry.getDomain( cell));
        }

        for( String cell : DOMAIN_2_CELLS) {
            String domain = _registry.getDomain( cell);
            assertEquals(DOMAIN_2, domain);
        }

        Set<String> domains = new HashSet<>(_registry.getDomains());
        Set<String> expectedDomains = Collections.singleton(DOMAIN_2);
        assertEquals(expectedDomains, domains);
    }

    private void simulateDomainRegistration(String domain, int timeout,
                                            String... cells) {
        simulateDomainRegistration(domain, timeout, Arrays.asList( cells));
    }

    private void simulateDomainRegistration(String domain, int timeout,
                                            Collection<String> cells) {
        RegistrationMessage message = new RegistrationMessage();
        message.setDomainName(domain);
        message.setCells(cells);
        message.setTimeout(timeout);

        _registry.onMessage(message);
    }

    /**
     * Provide a minimal implementation of read-only StreamMessage to
     * simulate a domain sending a message to the CNS. Write methods are not
     * supported. Read methods simulate the structure the message.  The basic
     * getters of the Message interface return dummy values.
     *
     * This message enforces the following structure:
     *
     * Read  Type    Description
     *  1    String  Domain-name
     *  2    Long    Time-out
     *  3    Int     Number of cells
     *  4    String  Cell No 1
     *  5    String  Cell No 2
     *  ...  ...     ...
     */
    private static class RegistrationMessage implements StreamMessage {
        private String _domainName;
        private long _timeout;
        private int _readCount;
        private List<String> _cells = new ArrayList<>();

        public void setDomainName( String name) {
            _domainName = name;
        }

        public void setTimeout( long timeout) {
            _timeout = timeout;
        }

        public void setCells( Collection<String> cells) {
            _cells.clear();
            _cells.addAll( cells);
        }

        @Override
        public String toString() {
            return "domain=" + _domainName + " (" + _timeout + ") cells=" +
                   Iterators.toString( _cells.iterator());
        }

        @Override
        public String readString() throws JMSException {
            _readCount++;

            if( _readCount == 1) {
                return _domainName;
            } else if( _readCount >= 4) {
                int index = _readCount - 4;

                if( index < _cells.size()) {
                    return _cells.get( index);
                }
            }

            throw new MessageFormatException( "No String for this read: " +
                                              _readCount);
        }

        @Override
        public long readLong() throws JMSException {
            _readCount++;

            if( _readCount != 2) {
                throw new MessageFormatException( "No Long for this read: " +
                                                  _readCount);
            }

            return _timeout;
        }

        @Override
        public int readInt() throws JMSException {
            _readCount++;

            if( _readCount != 3) {
                throw new MessageFormatException( "No Int for this read: " +
                                                  _readCount);
            }

            return _cells.size();
        }

        @Override
        public void acknowledge() throws JMSException {
        }

        @Override
        public void clearBody() throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void clearProperties() throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public boolean getBooleanProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public byte getByteProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public double getDoubleProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public float getFloatProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public int getIntProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public String getJMSCorrelationID() throws JMSException {
            return null;
        }

        @Override
        public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
            return null;
        }

        @Override
        public int getJMSDeliveryMode() throws JMSException {
            return 0;
        }

        @Override
        public Destination getJMSDestination() throws JMSException {
            return null;
        }

        @Override
        public long getJMSExpiration() throws JMSException {
            return 0;
        }

        @Override
        public String getJMSMessageID() throws JMSException {
            return null;
        }

        @Override
        public int getJMSPriority() throws JMSException {
            return 0;
        }

        @Override
        public boolean getJMSRedelivered() throws JMSException {
            return false;
        }

        @Override
        public Destination getJMSReplyTo() throws JMSException {
            return null;
        }

        @Override
        public long getJMSTimestamp() throws JMSException {
            return 0;
        }

        @Override
        public String getJMSType() throws JMSException {
            return null;
        }

        @Override
        public long getLongProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public Object getObjectProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Enumeration getPropertyNames() throws JMSException {
            return Iterators.asEnumeration( Iterators.emptyIterator());
        }

        @Override
        public short getShortProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public String getStringProperty( String arg0) throws JMSException {
            throw new MessageFormatException( "Property not found");
        }

        @Override
        public boolean propertyExists( String arg0) throws JMSException {
            return false;
        }

        @Override
        public void setBooleanProperty( String arg0, boolean arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setByteProperty( String arg0, byte arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setDoubleProperty( String arg0, double arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setFloatProperty( String arg0, float arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setIntProperty( String arg0, int arg1) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSCorrelationID( String arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSCorrelationIDAsBytes( byte[] arg0)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSDeliveryMode( int arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSDestination( Destination arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSExpiration( long arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSMessageID( String arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSPriority( int arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSRedelivered( boolean arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSReplyTo( Destination arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSTimestamp( long arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setJMSType( String arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setLongProperty( String arg0, long arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setObjectProperty( String arg0, Object arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setShortProperty( String arg0, short arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void setStringProperty( String arg0, String arg1)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public boolean readBoolean() throws JMSException {
            throw new MessageFormatException( "Cannot read a boolean here");
        }

        @Override
        public byte readByte() throws JMSException {
            throw new MessageFormatException( "Cannot read a byte here");
        }

        @Override
        public int readBytes( byte[] arg0) throws JMSException {
            throw new MessageFormatException( "Cannot read bytes here");
        }

        @Override
        public char readChar() throws JMSException {
            throw new MessageFormatException( "Cannot read a char here");
        }

        @Override
        public double readDouble() throws JMSException {
            throw new MessageFormatException( "Cannot read a double here");
        }

        @Override
        public float readFloat() throws JMSException {
            throw new MessageFormatException( "Cannot read a float here");
        }

        @Override
        public Object readObject() throws JMSException {
            throw new MessageFormatException( "Cannot read an object here");
        }

        @Override
        public short readShort() throws JMSException {
            throw new MessageFormatException( "Cannot read a short here");
        }

        @Override
        public void reset() throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeBoolean( boolean arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeByte( byte arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeBytes( byte[] arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeBytes( byte[] arg0, int arg1, int arg2)
                throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeChar( char arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeDouble( double arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeFloat( float arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeInt( int arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeLong( long arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeObject( Object arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeShort( short arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }

        @Override
        public void writeString( String arg0) throws JMSException {
            throw new MessageNotWriteableException( "Cannot adjust message");
        }
    }
}
