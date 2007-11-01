// $Id: SRMLsClientV2.java,v 1.10 2007-03-13 02:29:54 litvinse Exp $

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
import diskCacheV111.srm.FileMetaData;
import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.RequestStatus;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.client.SRMClientV2;
import org.ietf.jgss.GSSCredential;


import org.apache.axis.types.URI;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.handler.SrmLs;

public class SRMLsClientV2 extends SRMClient {
   
   private org.ietf.jgss.GSSCredential cred = null;
   
   private GlobusURL surls[];
   private String surl_strings[];
   private ISRM srm;
   /** Creates a new instance of SRMGetClient */
   public SRMLsClientV2(Configuration configuration, GlobusURL[] surls, String[] surl_strings) {
      super(configuration);
      this.surls = surls;
      this.surl_strings=surl_strings;
      
      try {cred = getGssCredential();}
      catch (Exception e) {
         cred = null;
         System.err.println("Couldn't getGssCredential.");
      }
   }
   
   public void connect() throws Exception {
       GlobusURL srmUrl = surls[0];
       srm = new SRMClientV2(srmUrl, getGssCredential(),configuration.getRetry_timeout(),configuration.getRetry_num(),configuration.getLogger(),doDelegation, fullDelegation,gss_expected_name,configuration.getWebservice_path());
      // Maybe we'll need this back again...
      // connect(surls[0]);
   }
   
   public void start() throws Exception {
      try {
         if(cred.getRemainingLifetime() < 60) 
            throw new Exception(
             "Remaining lifetime of credential is less than a minute.");
      }
      catch(org.ietf.jgss.GSSException gsse) {throw gsse;}
      
     SrmLsRequest req = new SrmLsRequest();
     req.setAllLevelRecursive(Boolean.FALSE);
     req.setFullDetailedList(Boolean.valueOf(configuration.isLongLsFormat()));
     req.setNumOfLevels(new Integer(configuration.getRecursionDepth()));
     req.setOffset(new Integer(configuration.getLsOffset()));
     req.setCount(new Integer(configuration.getLsCount()));
     org.apache.axis.types.URI[] turlia = new org.apache.axis.types.URI[surls.length];
     for(int i =0; i<surls.length; ++i) {
         turlia[i] = new org.apache.axis.types.URI(surl_strings[i]);
     }
     req.setArrayOfSURLs(new ArrayOfAnyURI(turlia));
     SrmLsResponse resp = srm.srmLs(req);
     if(resp == null){
         throw new Exception ("srm ls response is null!");
     }
     
     StringBuffer sb = new StringBuffer();


     if(resp.getReturnStatus().getStatusCode() != TStatusCode.SRM_SUCCESS) {
         sb.append(
            "Response from call to srmls:\n" +
            "Return status:\n" +
            " - Status code:  " +
	    resp.getReturnStatus().getStatusCode().getValue() + '\n' +
            " - Explanation:  " + resp.getReturnStatus().getExplanation() );
     }
     
     if(resp.getDetails() == null){
	 System.out.println(sb.toString());
         throw new Exception("srm ls response path details array is null!");
     }
     else { 
	 if (resp.getDetails().getPathDetailArray()!=null) {  
	     TMetaDataPathDetail[] details = resp.getDetails().getPathDetailArray();
	     SrmLs.printResults(sb,details,0," ",configuration.isLongLsFormat());
	 }
     }
     System.out.println(sb.toString());
     
   }
   
   public org.apache.axis.types.URI getTSURLInfo(String surl) throws Exception {
            
     org.apache.axis.types.URI uri = new org.apache.axis.types.URI(surl);
     //turli.setStorageSystemInfo(tssi);

     return uri;
   }
}

// $Log: not supported by cvs2svn $
// Revision 1.9  2007/03/12 21:11:14  litvinse
// introduced offset and count to srmLs and fixed SrmRmdir return code if
// non-empty directory is tried to be deleted
//
// Revision 1.8  2006/06/21 03:40:27  timur
// updated client to wsdl2.2, need to get latest wsdl next
//
// Revision 1.7  2006/03/24 00:29:03  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.6  2006/03/22 01:03:11  timur
// use abort files instead or release in case of interrupts or failures, better CTRL-C handling
//
// Revision 1.5  2006/03/18 00:41:34  timur
// srm v2 bug fixes
//
// Revision 1.4  2006/03/15 23:35:58  timur
// moved srm result printout into the srm handler
//
// Revision 1.3  2006/03/14 18:10:04  timur
// moving toward the axis 1_3
//
// Revision 1.2  2006/02/23 22:22:05  neha
// Changes by Neha- For Version 2- allow user specified value of command line option 'webservice_path'
// to override any default value.
//
// Revision 1.1  2005/12/13 23:07:52  timur
// modifying the names of classes for consistency
//
// Revision 1.4  2005/11/10 22:57:38  timur
// better printing in ls
//
// Revision 1.3  2005/11/09 23:53:59  timur
// added options for control of srmls output mode and recursion depth
//
// Revision 1.2  2005/11/08 00:58:06  timur
// print subdirectories
//
// Revision 1.1  2005/10/12 22:50:39  timur
// srmls
//
// Revision 1.4  2005/07/27 14:53:28  leoheska
// Minor improvements.
//
// Revision 1.3  2005/06/30 22:44:34  leoheska
// rudimentary srm-ls functionality complete
//
// Revision 1.2  2005/06/30 20:01:04  leoheska
// more changes to support srm-ls functionality
//
// Revision 1.1  2005/06/29 22:30:18  leoheska
// updates to support new srm functionality
//
