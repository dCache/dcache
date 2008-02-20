package org.dcache.tests.xrootd;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.junit.Test;


public class MessageTest {

    /**
     * A helper class which allows user-friendly serialization of primitive data types.
     * After choosing one of the constructors, the passed piece of data will be serialized according to
     * the xrootd protocol and can be obtained via <code> getData()</code>.
     *
     * This facility comes in handy to simulate incoming xrootd messages over the wire.
     */
    public static class HelperResponseMessage extends AbstractResponseMessage{
        
        /**
         * Create a new response which holds only a serialized long (8 bytes) in the data part.
         * @param l the long value to be serialized
         */
        public HelperResponseMessage(long l) {
            super(0, 0, 8);
            putSignedLong(l);
        }
        
        /**
         * Create a new response which holds only a serialized int (4 bytes) in the data part.
         * @param i the int value to be serialized
         */
        public HelperResponseMessage(int i) {
            super(0, 0, 4);
            putSignedInt(i);           
        }
        
        /**
         * Create a new response which holds some byte array in the data part.
         * @param sequence the plain byte array
         */
        public HelperResponseMessage(byte[] sequence) {
            super(0, 0, sequence.length);
            put(sequence);
        }
        
        /**
         * Create a new response which holds only a serialized String (a sequence of unsigned chars)
         * in the data part.
         * @param string the string to be serialized
         */
        public HelperResponseMessage(String String) {
            super(0, 0, String.length());
            putCharSequence(String);
        }
    }
    
    /**
     * Another helper class to make the internal de-serializing methods of the abstract base class
     * 'AbstractRequestMessage' accessable to the test framework.  
     *
     */
    public static class HelperRequestMessage extends AbstractRequestMessage {

        private HelperRequestMessage(int[] h, byte[] d) {
            super(h, d);
        }

        /**
         * Use this method to create a (simulated) xrootd request holding some data (payload). 
         * The header is set accordingly to announce the length of the data part. 
         * @param data the payload
         * @return an instance of HelperRequestMessage 
         */
        public static HelperRequestMessage getInstanceWithData (byte[] data) {
            int[] header = new int[XrootdProtocol.CLIENT_REQUEST_LEN];
            int len = data.length;
            
            header[20] = (len >> 24) & 0xff;
            header[21] = (len >> 16) & 0xff;
            header[22] = (len >>  8) & 0xff;
            header[23] = (len      ) & 0xff;
            
            return new HelperRequestMessage (header, data);
        }
        
        /**
         * Use this method to create a (simulated) xrootd request with certain header but with no data.
         * @param header the request header
         * @return an instance of HelperRequestMessage
         */
        public static HelperRequestMessage getInstanceWithHeader( int[] header) {
            return new HelperRequestMessage (header, null);
        }

        /**
         * Deserialize a long from the request. 8 Bytes will be scanned in Big-Endian and interpreted
         * as a signed long.
         * @return the java long representing the xrootd signed long
         */
        public long getLong() {
          
            return getLong(0, false);
        }
        
        /**
         * Deserialize a long from the request. 8 Bytes will be scanned in Big-Endian and interpreted
         * as a signed long. 
         * @param position the offset in the message array where to start scanning
         * @param fromHeader true if the value is read from the header array, false for the data array
         * @return the java long representing the xrootd signed long
         */
        public long getLong(int position, boolean fromHeader) {
            
            readFromHeader(fromHeader);
            return getSignedLong(position);
        }
        /**
         * @see <code>getLong()</code>
         * @return the java int representing the xrootd signed int
         */
        public int getInt() {
            return getInt(0, false);
        }

        /**
         * @see <code>getLong(int, boolean)</code>
         * @return the java int representing the xrootd signed int
         */
        public int getInt(int position, boolean fromHeader) {
            
            readFromHeader(fromHeader);
            return getSignedInt(position);
        }
        
        /**
         * Reads a string, which is encoded in the entire data part. 
         * @return the String
         */
        public String getString() {
            
            readFromHeader(false);
            
            StringBuffer sb = new StringBuffer();
            
            for (int i = 0; i < data.length; i++)   {
                sb.append((char) getUnsignedChar(i));
            }
            return sb.toString();
        }
    }

    //
    //
    //
    // The actual tests
    //
    //
    //
    
    /**
     * Test the creation of a basic response by putting some data in it.
     */

    @Test
    public void testResponseData() {
        byte someData[] = new byte[1024];
        for (int i = 0; i< someData.length;i++) {
            someData[i] = (byte) i;
        }
        
        AbstractResponseMessage msg = new HelperResponseMessage(someData);
        
        assertEquals(someData.length, msg.getDataLength());
        assertArrayEquals(someData, msg.getData());
    }
        
   
    
    
    /**
     * Test the serialization and deserialization of a java int using the helper classes.
     * This excercises AbstractResponseMessage.putSignedInt() and 
     * AbstractRequestMessage.getSignedInt() methods.
     */
    
    @Test
    public void testSignedInt() {
        
        // test with max positive integer
        int intValue = Integer.MAX_VALUE;
        byte[] someData = new HelperResponseMessage(intValue).getData();
        HelperRequestMessage req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(Integer.SIZE / 8, someData.length);
        assertEquals(Integer.SIZE / 8, req.getDataLength());
        assertEquals(intValue, req.getInt());
       
        // test with max negative integer
        intValue = Integer.MIN_VALUE;
        someData = new HelperResponseMessage(intValue).getData();
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(intValue, req.getInt());
        
        // simulate an incoming request holding a max unsigned int (2^32 - 1)
        // which should be interpreted by Java as -1
        someData = new byte[4];
        Arrays.fill(someData, (byte) 0xff);        
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(-1, req.getInt());
        
        // the same test as the last one, but this time in the header
        int[] header = new int[XrootdProtocol.CLIENT_REQUEST_LEN];
        Arrays.fill(header, 4, 8, 0xff);
        req = HelperRequestMessage.getInstanceWithHeader(header);
        assertEquals(-1, req.getInt(4, true));
        
        // try the following int in the data: 16843009 (dec) <-> 01010101 (hex)
        someData = new byte[4];
        Arrays.fill(someData, (byte) 0x01);        
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(16843009, req.getInt());
        
        // the same test as the last one, but this time in the header
        header = new int[XrootdProtocol.CLIENT_REQUEST_LEN];
        Arrays.fill(header, 4, 8, 0x01);
        req = HelperRequestMessage.getInstanceWithHeader(header);
        assertEquals(16843009, req.getInt(4, true));
        
     }     
     
    
    
    /**
     * Test the serialization and deserialization of a java long using the helper classes.
     * This excercises <code>AbstractResponseMessage.putSignedLong()</code> and 
     * <code>AbstractRequestMessage.getSignedLong()</code> methods.
     */
    
    @Test
    public void testSignedLong() {
        
        // test with max positive long
        long longValue = Long.MAX_VALUE;
        byte[] someData = new HelperResponseMessage(longValue).getData();
        HelperRequestMessage req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(Long.SIZE / 8, someData.length);
        assertEquals(Long.SIZE / 8, req.getDataLength());
        assertEquals(longValue, req.getLong());
        
        // test with max negative long
        longValue = Long.MIN_VALUE;
        someData = new HelperResponseMessage(longValue).getData();
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(longValue, req.getLong());
        
        // simulate an incoming request holding a max unsigned long (2^64 - 1)
        // which should be interpreted by Java as -1
        someData = new byte[8];
        Arrays.fill(someData, (byte) 0xff);
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(-1, req.getLong());

        // the same test as the last one, but this time in the header
        int[] header = new int[XrootdProtocol.CLIENT_REQUEST_LEN];
        Arrays.fill(header, 4, 12, 0xff);
        req = HelperRequestMessage.getInstanceWithHeader(header);
        assertEquals(-1, req.getLong(4, true));
        
        // try the following long in the data: 72340172838076673 (dec) <-> 0101010101010101 (hex)
        someData = new byte[8];
        Arrays.fill(someData, (byte) 0x01);
        req = HelperRequestMessage.getInstanceWithData(someData);
        assertEquals(72340172838076673L, req.getLong());        
        
        // the same test as the last one, but this time in the header
        header = new int[XrootdProtocol.CLIENT_REQUEST_LEN];
        Arrays.fill(header, 4, 12, 0x01);
        req = HelperRequestMessage.getInstanceWithHeader(header);
        assertEquals(72340172838076673L, req.getLong(4, true));
        
    }
    
    
    /**
     * Test serialization and deserialization of a string
     */
    
    @Test
    public void testString() {

        String testString = "abcxyz012?!@#%^_-+=:./";
        
        byte someData[] = new HelperResponseMessage(testString).getData();
        HelperRequestMessage msg = HelperRequestMessage.getInstanceWithData(someData);
        
        assertEquals(testString.length(), someData.length);
        assertEquals(testString.length(), msg.getDataLength());
        assertEquals(testString, msg.getString());
    }
}
