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

// $Id: GFtpPerfMarkersBlock.java,v 1.2 2005-10-26 17:56:41 aik Exp $

package org.dcache.ftp.door;

/**
 * <p>Title: GFtpPerfMarkersBlock</p>
 * <p>Description: Block of GridFtp Performance Markers</p>
 * <p>Copyright: Copyright (c) 2005</p>
 * <p>Company: FNAL</p>
 *
 * @author AIK
 * @version 1.0
 */

public class GFtpPerfMarkersBlock {
    public int getLength() { return (markers == null) ? 0: markers.length; }

    // By RFC definition, performance marker _includes_ number of streams in transfer.
    // This number may be not known apriory, and array is created with the length of Max number of streams allowed
    // Later when data streams are opened, the number of open streams can be less then max.

    private int     count;   // Current number of Performance Markers defined <= max
                                        //   equals numbers of open data streams
//    private int     max;     // Maximum number of performance markers, size of the GFtpPerfMarker[]

    private GFtpPerfMarker[] markers;
    public  GFtpPerfMarker[] getGFtpPerfMarkers() { return markers; }

    public  GFtpPerfMarker markers( int idx)
            throws IllegalArgumentException {
        if( idx < 0 || idx > markers.length ) {
            throw new IllegalArgumentException();
        } else {
            return markers[idx];
        }
    }
    /** Default constructor - empty block */
    public GFtpPerfMarkersBlock() {
        clear();
    }
    /** Constructor - create block of markers of specified length */
    public GFtpPerfMarkersBlock( int length ) {
        createNewBlock( length );
    }
    private void createNewBlock( int length ){
        if ( length > 0 ) {
            count = length;

            markers = new GFtpPerfMarker[length];
            for (int jStripe = 0; jStripe < length; jStripe++) {
                this.markers[jStripe] = new GFtpPerfMarker(jStripe, length );
            }
        }else{
            clear();
        }
    }

    /** Copy constructor */
    GFtpPerfMarkersBlock( GFtpPerfMarkersBlock pm ){
        int length = pm.getLength();
        count   = pm.getCount();
        count   = (count < length) ? count : length;

        if( pm.markers != null ) {
            markers = new GFtpPerfMarker[length];
            for (int jStripe = 0; jStripe < length; jStripe++) {
                this.markers[jStripe] = new GFtpPerfMarker(jStripe, pm.count);
                this.markers[jStripe].setBytesWithTime( pm.markers[jStripe].getstripeBytesTransferred(),
                        pm.markers[jStripe].getTimeStamp() );
            }
        }else{
            clear();
        }
    }
    public void setCount( int c ) {
        if( markers != null ) {
            count = (c < markers.length) ? c : markers.length;
        } else {
            count = 0;
        }
    }
    public int getCount()  { return count; }

    public void clear(){
        count   = 0;
        markers = null;
    }

    public long getBytesTransferred()
    {
        long sum = 0;
        for (GFtpPerfMarker marker : markers) {
            sum += marker.getstripeBytesTransferred();
        }
        return sum;
    }
}
