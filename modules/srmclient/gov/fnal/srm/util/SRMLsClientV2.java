// $Id: SRMLsClientV2.java 10382 2008-10-16 15:33:29Z litvinse $

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
import java.text.DateFormat;

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
     if (configuration.getLsCount()!=null) { 
             req.setCount(configuration.getLsCount());
     }
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
            "Return status:\n" +
            " - Status code:  " +
	    resp.getReturnStatus().getStatusCode().getValue() + '\n' +
            " - Explanation:  " + resp.getReturnStatus().getExplanation() );
	 throw new Exception(sb.toString());
     }
     
     if(resp.getDetails() == null){
	 System.out.println(sb.toString());
         throw new Exception("srm ls response path details array is null!");
     }
     else { 
	 if (resp.getDetails().getPathDetailArray()!=null) {  
	     TMetaDataPathDetail[] details = resp.getDetails().getPathDetailArray();
	     printResults(sb,details,0," ",configuration.isLongLsFormat());
	 }
     }
     System.out.println(sb.toString());
     
   }
   
   public org.apache.axis.types.URI getTSURLInfo(String surl) throws Exception {
            
     org.apache.axis.types.URI uri = new org.apache.axis.types.URI(surl);
     //turli.setStorageSystemInfo(tssi);

     return uri;
   }

    public static void printResults(StringBuffer sb,
				    TMetaDataPathDetail[] ta,
				    int depth,
				    String depthPrefix,
				    boolean longFormat) {
        if  (ta != null) {
            for (int i = 0; i < ta.length; i++) {
                TMetaDataPathDetail metaDataPathDetail = ta[i];
                if(metaDataPathDetail != null){
		    //sb.append(metaDataPathDetail.getStatus().getStatusCode()+" "+metaDataPathDetail.getStatus().getExplanation());
                    if (metaDataPathDetail.getStatus().getStatusCode() ==
			TStatusCode.fromString(TStatusCode._SRM_INVALID_PATH)) {

			    sb.append(TStatusCode._SRM_INVALID_PATH).append(" ").append(depthPrefix).append(" File/directory " + i + " " +
						      metaDataPathDetail.getPath() + " does not exist. \n" );
}
		    else {
                        sb.append(depthPrefix);
                        org.apache.axis.types.UnsignedLong size =metaDataPathDetail.getSize();
                        if(size != null) {
                            sb.append(" ").append( size.longValue());
                        }
                        sb.append(" ").append( metaDataPathDetail.getPath());
			if (metaDataPathDetail.getType()==TFileType.DIRECTORY) {
			    sb.append("/");
			}
                        if (metaDataPathDetail.getStatus().getStatusCode()!=TStatusCode.SRM_SUCCESS){
                                sb.append(" ("+metaDataPathDetail.getStatus().getStatusCode()+","+metaDataPathDetail.getStatus().getExplanation()+")");
                        }
                        sb.append('\n');
                        if(longFormat) {
			    sb.append(" space token(s) :");
			    if (metaDataPathDetail.getArrayOfSpaceTokens()!=null) {
				for (int j=0;j<metaDataPathDetail.getArrayOfSpaceTokens().getStringArray().length;j++) {
				    if (j==metaDataPathDetail.getArrayOfSpaceTokens().getStringArray().length-1) {
					sb.append(metaDataPathDetail.getArrayOfSpaceTokens().getStringArray()[j]);
				    }
				    else {
					sb.append(metaDataPathDetail.getArrayOfSpaceTokens().getStringArray()[j]+",");
				    }
				}
			    }
			    else {
				sb.append("none found");
			    }
			    sb.append('\n');
                            TFileStorageType stortype= metaDataPathDetail.getFileStorageType();
                            if(stortype != null) {
                                sb.append(depthPrefix);
                                sb.append(" storage type:").append(stortype.getValue());
                                sb.append('\n');
                            }
			    else {
				sb.append(" type: null");
				sb.append('\n');
			    }
			    TRetentionPolicyInfo rpi = metaDataPathDetail.getRetentionPolicyInfo();
			    if (rpi != null) {
				TRetentionPolicy rt = rpi.getRetentionPolicy();
				if (rt != null) {
				    sb.append(depthPrefix);
				    sb.append(" retention policy:").append(rt.getValue());
				    sb.append('\n');
				}
				else {
				    sb.append(" retention policy: null");
				    sb.append('\n');
				}
				TAccessLatency al = rpi.getAccessLatency();
				if (al != null) {
				    sb.append(depthPrefix);
				    sb.append(" access latency:").append(al.getValue());
				    sb.append('\n');
				}
				else {
				    sb.append(" access latency: null");
				    sb.append('\n');
				}
			    }
			    else {
				sb.append(" retentionpolicyinfo : null");
				sb.append('\n');
			    }
                            TFileLocality locality =  metaDataPathDetail.getFileLocality();
                            if(locality != null) {
                                sb.append(depthPrefix);
                                sb.append(" locality:").append(locality.getValue());
                                sb.append('\n');
                            }
			    else {
				sb.append(" locality: null");
				sb.append('\n');
			    }
                            if (metaDataPathDetail.getCheckSumValue() != null) {
                                sb.append(depthPrefix).append( " - Checksum value:  " +
                                        metaDataPathDetail.getCheckSumValue() + '\n');
                            }

                            if (metaDataPathDetail.getCheckSumType() != null) {
                                sb.append(depthPrefix).append( " - Checksum type:  " +
                                        metaDataPathDetail.getCheckSumType() + '\n');
                            }
                            java.text.SimpleDateFormat df =
                                    new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                            java.text.FieldPosition tfp =
                                    new java.text.FieldPosition(DateFormat.FULL);


                           if (metaDataPathDetail.getOwnerPermission() != null) {
                                 TUserPermission up =
                                    metaDataPathDetail.getOwnerPermission();
                                    sb.append(depthPrefix).append("  UserPermission:");
                                    sb.append(" uid=").append( up.getUserID() );
                                    sb.append(" Permissions");
                                    sb.append(up.getMode().getValue());
                                    sb.append('\n');
                           }


                           if (metaDataPathDetail.getGroupPermission() != null) {
                                TGroupPermission gp =
                                metaDataPathDetail.getGroupPermission();
                                    sb.append(depthPrefix).append("  GroupPermission:");
                                    sb.append(" gid=").append( gp.getGroupID() );
                                    sb.append(" Permissions");
                                    sb.append(gp.getMode().getValue());
                                    sb.append('\n');
                           }
                          if(metaDataPathDetail.getOtherPermission() != null)
                          {
                                sb.append(depthPrefix).append(" WorldPermission: ");
                                sb.append(metaDataPathDetail.getOtherPermission().getValue());
                                sb.append('\n');
                          }


                            if (metaDataPathDetail.getCreatedAtTime() != null) {
                                java.util.Date tdate = metaDataPathDetail.getCreatedAtTime().getTime();
                                if (tdate != null) {
                                    StringBuffer dsb = new StringBuffer();
                                    df.format(tdate, dsb, tfp);
                                    sb.append(depthPrefix).append("created at:").append(dsb);
                                    sb.append('\n');
                                }
                            }
                            if (metaDataPathDetail.getLastModificationTime() != null) {
                                java.util.Date tdate =
                                        metaDataPathDetail.getLastModificationTime().getTime();
                                if (tdate != null)  {
                                    StringBuffer dsb = new StringBuffer();
                                    df.format(tdate, dsb, tfp);
                                    sb.append(depthPrefix);
                                    sb.append("modified at:").append(dsb);
                                    sb.append('\n');
                                }
                            }


                            if(metaDataPathDetail.getLifetimeAssigned()!= null)
                                sb.append(depthPrefix).append("  - Assigned lifetime (in seconds):  " +
                                        metaDataPathDetail.getLifetimeAssigned() + '\n');

                            if(metaDataPathDetail.getLifetimeLeft()!= null)
                                sb.append(depthPrefix).append( " - Lifetime left (in seconds):  " +
                                        metaDataPathDetail.getLifetimeLeft() + '\n');

                            sb.append(depthPrefix).append(
                                    " - Original SURL:  " +
                                    metaDataPathDetail.getPath() + '\n' +
                                    " - Status:  " + metaDataPathDetail.getStatus().getExplanation() +
                                    '\n' +
                                    " - Type:  " + metaDataPathDetail.getType() + '\n');
                        }


                        if (metaDataPathDetail.getArrayOfSubPaths() != null) {
                            TMetaDataPathDetail subpaths[] =metaDataPathDetail.getArrayOfSubPaths().getPathDetailArray();
                            if(subpaths ==ta) {
                                sb.append(depthPrefix).append( " circular subpath reference !!!");

                            } else {
                                printResults(sb,subpaths,depth+1,depthPrefix+"    ",longFormat);
                            }
                        }
                    }
                }
            }
        }
    }

}

