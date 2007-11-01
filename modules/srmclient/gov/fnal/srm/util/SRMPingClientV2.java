// $Id: SRMPingClientV2.java,v 1.2 2007-10-23 19:45:55 litvinse Exp $
// $Log: not supported by cvs2svn $
// Revision 1.1  2006/10/03 18:41:45  timur
// added srmBringOnline and srmPing clients, still untested
//
// Revision 1.18  2006/09/06 15:53:46  timur
//  reformated code, improved error reporting
//
// Revision 1.17  2006/06/21 20:31:56  timur
// Upgraded code to the latest srmv2.2 wsdl (final)" src wsdl sbin/srmv2.2-deploy.wsdd
//
// Revision 1.16  2006/06/21 03:40:27  timur
// updated client to wsdl2.2, need to get latest wsdl next
//
// Revision 1.15  2006/04/21 22:54:29  timur
// better debug info printout
//
// Revision 1.14  2006/03/24 00:29:03  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.13  2006/03/22 01:03:11  timur
// use abort files instead or release in case of interrupts or failures, better CTRL-C handling
//
// Revision 1.12  2006/03/18 00:41:34  timur
// srm v2 bug fixes
//
// Revision 1.11  2006/03/14 18:10:04  timur
// moving toward the axis 1_3
//
// Revision 1.10  2006/02/27 22:54:25  timur
//  do not use Keep Space parameter in srmReleaseFiles, reduce default wait time
//
// Revision 1.9  2006/02/23 22:22:05  neha
// Changes by Neha- For Version 2- allow user specified value of command line option 'webservice_path'
// to override any default value.
//
// Revision 1.8  2006/02/08 23:21:58  neha
// changes by Neha. Added new command line option -storagetype
// Its values could be permanent,volatile or durable;permanent by default
//
// Revision 1.6  2006/01/24 21:14:47  timur
// changes related to the return code
//
// Revision 1.5  2006/01/20 21:50:33  timur
// remove unneeded connect
//
// Revision 1.4  2005/12/14 01:58:44  timur
// srmPrepareToGet client is ready
//
// Revision 1.3  2005/12/09 00:24:51  timur
// srmPrepareToGet works
//
// Revision 1.2  2005/12/08 01:02:07  timur
// working on srmPrepereToGet
//
// Revision 1.1  2005/12/07 02:05:22  timur
// working towards srm v2 get client
//
// Revision 1.21  2005/06/08 22:34:55  timur
// fixed a bug, which led to recognition of some valid file ids as invalid
//
// Revision 1.20  2005/04/27 19:20:55  timur
// make sure client works even if report option is not specified
//                                        String error = "srmPrepareToPut update failed, status : "+ statusCode+
// Revision 1.19  2005/04/27 16:40:00  timur
// more work on report added gridftpcopy and adler32 binaries
//
// Revision 1.18  2005/04/26 02:06:08  timur
// added the ability to create a report file
//
// Revision 1.17  2005/03/11 21:18:36  timur
// making srm compatible with cern tools again
//
// Revision 1.16  2005/01/25 23:20:20  timur
// srmclient now uses srm libraries
//
// Revision 1.15  2005/01/11 18:19:29  timur
// fixed issues related to cern srm, make sure not to change file status for failed files
//
// Revision 1.14  2004/06/30 21:57:04  timur
//  added retries on each step, added the ability to use srmclient used by srm copy in the server, added srm-get-request-status
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
 * SRMGetClient.java
 *
 * Created on January 28, 2003, 2:54 PM
 */

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.RequestFileStatus;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.io.IOException;
import java.util.Iterator;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;
/**
 *
 * @author  timur
 */
public class SRMPingClientV2 extends SRMClient  {
    private ISRM srmv2;
    GlobusURL srmurl;
    /** Creates a new instance of SRMGetClient */
    public SRMPingClientV2(Configuration configuration, GlobusURL srmurl) {
        super(configuration);
        this.srmurl = srmurl;
    }
    
    
    public void connect() throws Exception {
        srmv2 = new SRMClientV2(srmurl,
                getGssCredential(),
                configuration.getRetry_timeout(),
                configuration.getRetry_num(),
                configuration.getLogger(),
                doDelegation,
                fullDelegation,
                gss_expected_name,
                configuration.getWebservice_path());
    }
    
    
    public void start() throws Exception {
        try {
            SrmPingRequest request = new SrmPingRequest();
            SrmPingResponse response = srmv2.srmPing(request);
            say("received response");
            if(response == null) {
                throw new IOException(" null response");
            }
	    StringBuffer sb = new StringBuffer();
	    sb.append("VersionInfo : "+response.getVersionInfo()+"\n");
	    if (response.getOtherInfo()!=null) { 
		ArrayOfTExtraInfo info = response.getOtherInfo();
		if (info.getExtraInfoArray()!=null) {
		    for (int i=0;i<info.getExtraInfoArray().length;i++) { 
			TExtraInfo extraInfo = info.getExtraInfoArray()[i];
			sb.append(extraInfo.getKey() +":"+(extraInfo.getValue())+"\n");
		    }
		}
	    }
	    say(sb.toString());
        }catch (Exception e){
            if(configuration.isDebug()) {
                e.printStackTrace();
            } else {
                esay(e.toString());
            }
            throw e;
        }
    }
    
}
