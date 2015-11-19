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

package org.dcache.ftp.door;

/**
 * <p>Title: GFtpPerfMarker.java</p>
 *
 * <p>Description: GridFTP Performance Marker Implementation</p>
 *
 * <p>Copyright: Copyright (c) 2005</p>
 *
 * <p>Company: FNAL</p>
 *
 * @author AIK
 * @version 1.0
 *
 *
 * $Id: GFtpPerfMarker.java,v 1.2 2005-10-26 17:56:41 aik Exp $
 */

/**
 *
 * NO SYNCHRONIZATION done (yet) !!!
 */
public class GFtpPerfMarker {
    private static final String _cvsId = "$Id: GFtpPerfMarker.java,v 1.2 2005-10-26 17:56:41 aik Exp $";

    private long _timeStamp;
    private long _stripeIndex;
    private long _stripeBytesTransferred;
    private long _totalStripeCount;

    /** Constructor */
    public GFtpPerfMarker( long stripeIndex, long totalStripeCount ) {
        _timeStamp        = System.currentTimeMillis();
        _stripeIndex      = stripeIndex;
        _totalStripeCount = totalStripeCount;
        _stripeBytesTransferred = 0;
    }

    // Getters
    //
    /** */
    public long getTimeStamp() { return _timeStamp; }
    /** */
    public long getStripeIndex() { return _stripeIndex; }
    /** */
    public long getstripeBytesTransferred() { return _stripeBytesTransferred; }
    /** */
    public long getStripeCount() { return _totalStripeCount; }

    // Setters
    //
    /** set Time Stamp*/
    public void setTimeStamp(long timeStamp) {
        _timeStamp = timeStamp;
    }
    /** update Time Stamp - set current time */
    public void updateTimeStamp() {
        _timeStamp = System.currentTimeMillis();
    }

    /** */
    public void setStripeBytesTransferred(long byteCount) {
        _stripeBytesTransferred = byteCount;
    }

    // More Setters
    //

    /** */
    public void setBytesWithTime(long byteCount, long time) {
        _stripeBytesTransferred = byteCount;
        _timeStamp = time;
    }

    /** Set counter stripeBytesTransferred by byteCountAdd and update timestamp
     */
    public void setBytesWithTime(long byteCount) {
        _stripeBytesTransferred = byteCount;
        _timeStamp = System.currentTimeMillis();
    }
    /** increment counter stripeBytesTransferred by byteCountAdd and update timestamp */
    public void addBytesWithTime(long byteCountAdd ) {
        _stripeBytesTransferred += byteCountAdd;
        _timeStamp = System.currentTimeMillis();
    }

    // Conversion to string
    //

    /** @return String formatted according gridftp protocol extension to be sent to the ftp control line
     */
    public String getReply(){
        long sec =  _timeStamp/1000;
        long hms = (_timeStamp%1000)/100; // hundreds of millisec; one digit only
        String s =
                "112-Perf Marker\r\n"
                        +" Timestamp:  " +sec +"." +hms+ "\r\n"
                        +" Stripe Index: "+_stripeIndex+"\r\n"
                        +" Stripe Bytes Transferred: "+_stripeBytesTransferred+"\r\n"
                        +" Total Stripe Count: "+_totalStripeCount+"\r\n"
                        +"112 End.";
        /** @todo: bug in grid ftp client implementation, it check for '.' at the end,
         * '.' is not in standard/
         */

        // Globus Grid Ftp has dot '.' in "112 End.\r\n",
        // this is not in standard GWD-R: GridFTP: Protocol Extensions to FTP... 4/2003
        return s;
    }

    /** @return String - one line printout */
    public String toString(){
        long sec =  _timeStamp/1000;
        long hms = (_timeStamp%1000); // hundreds of millisec; one digit only
        String s = "GFtpPerfMarker: Timestamp=" +sec +"." +hms
                +"; StripeIndex=" +_stripeIndex
                +"; StripeBytesTransferred=" +_stripeBytesTransferred
                +"; TotalStripeCount=" +_totalStripeCount
                +";";
        return s;
    }

}

//
// $Log: not supported by cvs2svn $
// Revision 1.1.2.2  2005/10/14 21:48:00  aik
// <No Comment Entered>
//
// Revision 1.1.2.1  2005/10/05 22:54:09  aik
// GridFtp Perfomance Marker class
//
//
