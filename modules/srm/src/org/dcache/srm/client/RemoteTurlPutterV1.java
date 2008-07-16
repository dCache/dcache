// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.1  2006/01/10 19:03:37  timur
// adding srm v2 built in client
//
// Revision 1.5  2005/08/29 22:52:04  timur
// commiting changes made by Neha needed by OSG
//
// Revision 1.4  2005/03/24 19:16:18  timur
// made built in client always delegate credentials, which is required by LBL's DRM
//
// Revision 1.3  2005/03/13 21:56:28  timur
// more changes to restore compatibility
//
// Revision 1.2  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.3  2005/01/07 20:55:30  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.6  2004/08/03 16:51:47  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.5  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.4  2004/06/16 19:44:32  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//
// Revision 1.1.2.3  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
//

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

/*
 * RemoteTurlGetter.java
 *
 * Created on April 30, 2003, 2:38 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.globus.util.GlobusURL;
import org.globus.io.urlcopy.UrlCopy;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.dcache.srm.client.SRMClientV1;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.RequestCredential;
import java.beans.PropertyChangeListener;
import diskCacheV111.srm.server.SRMServerV1;

/**
 *
 * @author  timur
 */
public class RemoteTurlPutterV1 extends TurlGetterPutterV1
{
    
    long[] sizes;
    
    public void say(String s) {
        storage.log("RemoteTurlPutterV1 :"+s);
    }
    
    public void esay(String s) {
        storage.elog("RemoteTurlPutterV1 :"+s);
    }
    
    public void esay(Throwable t) {
        storage.elog("RemoteTurlPutterV1 exception");
        storage.elog(t);
    }
    
    public RemoteTurlPutterV1(AbstractStorageElement storage,
    RequestCredential credential, String[] SURLs,
    long sizes[],
    String[] protocols,PropertyChangeListener listener,
    long retry_timeout,int retry_num , boolean connect_to_wsdl) {
        super(storage,credential,SURLs,protocols, connect_to_wsdl, retry_timeout,retry_num);
        addListener(listener);
        this.sizes = sizes;
   }
    
    protected diskCacheV111.srm.RequestStatus getInitialRequestStatus() 
        throws IOException,InterruptedException,javax.xml.rpc.ServiceException {
        boolean[] wantperm =
        new boolean[number_of_file_reqs];
        java.util.Arrays.fill(wantperm,true);
        return remoteSRM.put(SURLs,SURLs,sizes,wantperm,protocols);
    }    
}
