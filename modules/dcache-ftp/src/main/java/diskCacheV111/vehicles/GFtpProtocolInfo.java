/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/

package diskCacheV111.vehicles ;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.StandardProtocolFamily;

public class GFtpProtocolInfo implements IpProtocolInfo {
    private String _name  = "Unkown" ;
    private int    _minor;
    private int    _major;
    private InetSocketAddress _addr;
    private long   _transferTime;
    private long   _bytesTransferred;
    private String	_mode = "S";
    private int _parallelStart = 5;
    private int _parallelMin = 5;
    private int _parallelMax = 5;
    private int _bufferSize;
    private long _offset;
    private long _size;
    private String _checksumType = "Unknown";

    /**
     * The cell name of the FTP door handling the control channel.
     *
     * Added for GFtp/2. We rely on default initialisation to null.
     */
    private String _doorCellName;

    /**
     * The cell domain name of the FTP door handling the control
     * channel.
     *
     * Added for GFtp/2. We rely on default initialisation to null.
     */
    private String _doorCellDomainName;

    /**
     * The control channel address of the FTP client.
     *
     * Added for GFtp/2. We rely on default initialisation to null.
     */
    private String _clientAddress;

    /**
     * Whether the pool is requested to be passive.
     *
     * Kept for backwards compatibility with 2.8 and before.
     */
    @Deprecated
    private boolean _passive;

    /**
     * Whether the pool is requested to be passive.
     *
     * Added for GFtp/2. We rely on default initialisation to false. Like _passive,
     * but while _passive is only used for IPv4, this field is used for both IPv4
     * and IPv6.
     */
    private boolean _extendedPassive;

    /**
     * Protocol family for passive transfers.
     */
    private ProtocolFamily _protocolFamily;

    private static final long serialVersionUID = 5591743387114320262L;

    public GFtpProtocolInfo( String protocol, int major , int minor ,
                             InetSocketAddress addr, int start, int min,
                             int max, int bufferSize ,long offset, long size){
        _name  = protocol ;
        _minor = minor ;
        _major = major ;
        _addr  = addr ;
        _parallelStart = start;
        _parallelMin = min;
        _parallelMax = max;
        _bufferSize = bufferSize;
        _offset = offset;
        _size = size;
    }

    //
    //  the ProtocolInfo interface
    //
    public int getParallelStart() {return _parallelStart; }
    public int getMin() {return _parallelMin; }
    public int getMax() { return _parallelMax; }

    @Override
    public String getProtocol(){ return _name ; }
    @Override
    public int    getMinorVersion(){ return _minor ; }
    @Override
    public int    getMajorVersion(){ return _major ; }
    @Override
    public String getVersionString(){
        return _name+"-"+_major+"."+_minor ;
    }
    //
    // and the private stuff
    //
    public void   setBytesTransferred( long bytesTransferred ){
        _bytesTransferred = bytesTransferred ;
    }
    public void   setTransferTime( long transferTime ){
        _transferTime = transferTime ;
    }

    public void setBufferSize( int bufferSize ) {
        _bufferSize = bufferSize;
    }

    public int getBufferSize() { return _bufferSize; }
    public long getTransferTime(){ return _transferTime ; }
    public long getBytesTransferred(){ return _bytesTransferred ; }
    //
    public String toString(){  return getVersionString() +
            " " + _addr.getAddress().getHostAddress() +" "
            +  _addr.getPort();
    }
    //
    public void setMode(String mode)
    {	_mode = mode;	}
    public String getMode()
    {	return _mode;	}

    //offset of read
    public long getOffset()
    {
        return _offset;
    }

    //size of read
    public long getSize()
    {
        return _size;
    }

    /**
     * Returns the cell name of the FTP door. May be null.
     */
    public String getDoorCellName()
    {
        return _doorCellName;
    }

    /** Sets the cell name of the FTP door. */
    public void setDoorCellName(String name)
    {
        _doorCellName = name;
    }

    /**
     * Returns the cell domain name of the FTP door. May be null.
     */
    public String getDoorCellDomainName()
    {
        return _doorCellDomainName;
    }

    /** Sets the cell domain name of the FTP door. */
    public void setDoorCellDomainName(String name)
    {
        _doorCellDomainName = name;
    }

    /**
     * Returns the IP address of the client end of the control
     * channel.  May be null.
     */
    public String getClientAddress()
    {
        return _clientAddress;
    }

    /**
     * Sets the IP address of the client end of the control channel.
     */
    public void setClientAddress(String address)
    {
        _clientAddress = address;
    }

    /**
     * Returns whether the pool is requested to be passive.
     */
    public boolean getPassive()
    {
        return _extendedPassive;
    }

    /**
     * Sets whether the pool is requested to be passive.
     */
    public void setPassive(boolean passive)
    {
        _extendedPassive = passive;
    }

    public void setChecksumType(String f){
        _checksumType = f;
    }

    public String getChecksumType(){
        return _checksumType;
    }

    public void setProtocolFamily(ProtocolFamily protocolFamily)
    {
        _protocolFamily = protocolFamily;
    }

    public ProtocolFamily getProtocolFamily()
    {
        return _protocolFamily;
    }

    public void setSocketAddress(InetSocketAddress address) {
        _addr = address;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _addr;
    }

    /**
     * Ensures compatibility with pools from 2.8 and older. For old pools, passive mode
     * is only requested for IPv4.
     */
    private void writeObject (ObjectOutputStream out) throws IOException
    {
        _passive = _extendedPassive && _protocolFamily == StandardProtocolFamily.INET;
        out.defaultWriteObject();
    }
}
