// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.10  2006/01/19 01:48:21  timur
// more v2 copy work
//
// Revision 1.9  2006/01/12 23:38:10  timur
// first working version of srmCopy
//
// Revision 1.8  2006/01/10 19:03:37  timur
// adding srm v2 built in client
//
// Revision 1.7  2005/08/29 22:52:04  timur
// commiting changes made by Neha needed by OSG
//
// Revision 1.6  2005/07/19 01:13:38  leoheska
// More changes to srm.  Still not finished.
//
// Revision 1.5  2005/03/24 19:16:19  timur
// made built in client always delegate credentials, which is required by LBL's DRM
//
// Revision 1.4  2005/03/23 18:10:38  timur
// more space reservation related changes, need to support it in case of "copy"
//
// Revision 1.3  2005/03/13 21:56:29  timur
// more changes to restore compatibility
//
// Revision 1.2  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.6  2005/01/11 18:10:39  timur
// do not retry setFileStatus
//
// Revision 1.5  2005/01/07 20:55:30  timur
// changed the implementation of the built in client to use apache axis soap toolkit
//
// Revision 1.4  2004/10/28 02:41:30  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.3  2004/08/30 17:14:48  timur
// stop updating the status on the remote machine when the copy request is canceled, handle the queues more correctly
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.10  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.9  2004/07/29 22:17:29  timur
// Some functionality for disk srm is working
//
// Revision 1.1.2.8  2004/07/12 21:52:05  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.1.2.7  2004/07/09 22:14:54  timur
// more synchronization problems resloved
//
// Revision 1.1.2.6  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.5  2004/06/16 19:44:32  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//
// Revision 1.1.2.4  2004/06/15 22:15:41  timur
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
 * TurlGetterPutter.java
 *
 * Created on May 1, 2003, 12:41 PM
 */

package org.dcache.srm.client;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.Logger;
import org.globus.util.GlobusURL;
import org.globus.io.urlcopy.UrlCopy;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.OneToManyMap;
// import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.scheduler.*;
import org.dcache.srm.request.RequestCredential;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
/**
 *
 * @author  timur
 */
public abstract class TurlGetterPutter implements Runnable {
    private PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);
    
    public void notifyOfTURL(String SURL,String TURL,String requestId, String fileId,Long size) {
        say("notifyOfTURL( surl="+SURL+" , turl="+TURL+")");
        changeSupport.firePropertyChange(new TURLsArrivedEvent(this,SURL,TURL,requestId,fileId,size));
    }
    
    public void notifyOfFailure(String SURL,Object reason,String requestId, String fileId) {
        changeSupport.firePropertyChange(new TURLsGetFailedEvent(this,SURL,reason,requestId,fileId));
    }
    
    public void notifyOfFailure(Object reason) {
        changeSupport.firePropertyChange(new RequestFailedEvent(this,reason));
    }
    
    public void addListener(PropertyChangeListener listener) {
        changeSupport.addPropertyChangeListener(listener);
    }
    
    private Object sync = new Object();
    protected AbstractStorageElement storage;
    protected RequestCredential credential;
    protected String[] protocols;
    
    private boolean stopped = false;
    
    public abstract void say(String s);
    
    public abstract void esay(String s);
    
    public abstract void esay(Throwable t);
    
    /** Creates a new instance of RemoteTurlGetter */
    public TurlGetterPutter(AbstractStorageElement storage,
    RequestCredential credential,
    String[] protocols) {
        this.storage = storage;
        this.credential = credential;
        this.protocols = protocols;
    }
    
    
     public abstract void getInitialRequest() throws SRMException;
    
    
    
     
    /**
     * Getter for property stopped.
     * @return Value of property stopped.
     */
    public boolean isStopped() {
        return stopped;
    }
    
    /**
     * Setter for property stopped.
     * @param stopped New value of property stopped.
     */
    public void stop() {
        this.stopped = true;
    }
}
