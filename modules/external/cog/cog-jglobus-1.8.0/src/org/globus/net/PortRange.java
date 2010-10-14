/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.net;

import java.io.IOException;

import org.globus.common.CoGProperties;

/**
 * This class manages the port ranges. It keeps track of which
 * ports are used and which ones are open.
 */
public class PortRange {

    protected static final byte UNUSED = 0;
    protected static final byte USED   = 1;
    
    private boolean portRange = false;
    private int minPort, maxPort;
    private byte [] ports;
    
    private static PortRange tcpPortRange = null;
    private static PortRange tcpSourcePortRange = null;
    private static PortRange udpSourcePortRange = null;
    
    protected PortRange() {
    }
    
    /**
     * Returns PortRange instance for TCP listening sockets.
     *
     * @see #getTcpInstance()
     */
    public static PortRange getInstance() {
        return getTcpInstance();
    }
    
    /**
     * Returns PortRange instance for TCP listening sockets.
     * If the tcp.port.range property is set, the class
     * will be initialized with the specified port ranges.
     *
     * @return PortRange the default instace of this class.
     */
    public static synchronized PortRange getTcpInstance() {
        if (tcpPortRange == null) {
            tcpPortRange = new PortRange();
            tcpPortRange.init(CoGProperties.getDefault().getTcpPortRange());
        }
        return tcpPortRange;
    }
    
    /**
     * Returns PortRange instance for TCP source sockets.
     * If the tcp.source.port.range property is set, the class
     * will be initialized with the specified port ranges.
     *
     * @return PortRange the default instace of this class.
     */
    public static synchronized PortRange getTcpSourceInstance() {
        if (tcpSourcePortRange == null) {
            tcpSourcePortRange = new PortRange();
            tcpSourcePortRange.init( 
                  CoGProperties.getDefault().getTcpSourcePortRange());
        }
        return tcpSourcePortRange;
    }

    /**
     * Returns PortRange instance for UDP source sockets.
     * If the udp.source.port.range property is set, the class
     * will be initialized with the specified port ranges.
     *
     * @return PortRange the default instace of this class.
     */
    public static synchronized PortRange getUdpSourceInstance() {
        if (udpSourcePortRange == null) {
            udpSourcePortRange = new PortRange();
            udpSourcePortRange.init( 
                  CoGProperties.getDefault().getUdpSourcePortRange());
        }
        return udpSourcePortRange;
    }
    
    /**
     * Checks if the port range is set.
     *
     * @return true if the port range is set, false otherwise.
     */
    public boolean isEnabled() {
        return portRange;
    }
    
    /**
     * Returns first available port.
     *
     * @param lastPortNumber port number to start finding the next 
     *                       available port from. Set it to 0 if
     *                       called initialy.
     * @return the next available port number from the lastPortNumber.
     * @exception IOException if there is no more free ports available or
     *            if the lastPortNumber is incorrect.
     */
    public synchronized int getFreePort(int lastPortNumber) 
        throws IOException {
        int id = 0;
        if (lastPortNumber != 0) {
            id = lastPortNumber - minPort;
            if (id < 0) {
                throw new IOException("Port number out of range.");
            }
        }
        for(int i=id;i<ports.length;i++) {
            if (ports[i] == USED) continue;
            return minPort+i;
        }
        throw new IOException("No free ports available.");
    }
  
    /**
     * Sets the port number as used.
     *
     * @param portNumber port number 
     */
    public void setUsed(int portNumber) {
        setPort(portNumber, USED);
    }
  
    /**
     * Releases or frees the port number.
     * (Mark it as unused)
     * 
     * @param portNumber port number
     */
    public void free(int portNumber) {
        setPort(portNumber, UNUSED);
    }
    
    /**
     * Checks if the port number if within port range. Does <b>not</b>
     * check if it is a free, but only checks if it is within said range.
     */
    public boolean isInRange(int portNumber) {

        if ((minPort <= portNumber) && (maxPort >= portNumber)) {
            return true;
        }
        
        return false;
    }

    private synchronized void setPort(int portNumber, byte type) {
        int id = portNumber - minPort;
        if (id < 0) {
            throw new IllegalArgumentException("Port number out of range: " + portNumber);
        }
        ports[id] = type;
    }
    
    private void init(String portRangeStr) {
        portRange = false;
        
        if (portRangeStr == null) return ;
        
        int pos = portRangeStr.indexOf(",");
        if (pos == -1) {
            throw new IllegalArgumentException("Missing comma in the port range property: " +
                                               portRangeStr);
        }
        
        int min, max;
        
        try {
            min = Integer.parseInt(portRangeStr.substring(0, pos).trim());
        } catch(Exception e) {
            throw new IllegalArgumentException("The minimum port range value is invalid: " + 
                                               e.getMessage());
        }
        
        try {
            max = Integer.parseInt(portRangeStr.substring(pos+1).trim());
        } catch(Exception e) {
            throw new IllegalArgumentException("The maximum port range value is invalid: " + 
                                               e.getMessage());
        }
    
        if (min >= max) {
            throw new IllegalArgumentException("The minimum port range value is greater then " + 
                                               "the maximum port range value.");
        }
    
        minPort = min;
        maxPort = max;
        ports     = new byte[ maxPort-minPort ];
        portRange = true;    
    }
    
}
