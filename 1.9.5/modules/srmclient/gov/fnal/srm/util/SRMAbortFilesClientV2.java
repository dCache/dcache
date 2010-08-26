//______________________________________________________________________________
//
// $Id$
// $Author$ 
// 
// 
// created 04/08 by Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________ 

package gov.fnal.srm.util;
import java.io.IOException;
import org.globus.util.GlobusURL;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.util.RequestStatusTool;
import org.apache.axis.types.URI;

public class SRMAbortFilesClientV2 extends SRMClient {
	private ISRM srm;
	private org.ietf.jgss.GSSCredential credential = null;
	
	public SRMAbortFilesClientV2(Configuration configuration) {
		super(configuration);
	}

	public void connect() throws Exception { 
		credential=getGssCredential();
		srm = new SRMClientV2((new GlobusURL(configuration.getSrmUrl())), 
				      credential,
				      configuration.getRetry_timeout(),
				      configuration.getRetry_num(),
				      configuration.getLogger(),
				      doDelegation, 
				      fullDelegation,
				      gss_expected_name,
				      configuration.getWebservice_path());   
	}
	
	public void start() throws Exception{ 
		try {
			if (credential.getRemainingLifetime() < 60)
				throw new Exception(
					"Remaining lifetime of credential is less than a minute.");
		} 
		catch (org.ietf.jgss.GSSException gsse) {
			throw gsse;
		}
		StringBuffer sb = new StringBuffer();
		boolean failed=false;
		if (configuration.getArrayOfRequestTokens()!=null) { 
			for (String requestToken : configuration.getArrayOfRequestTokens()) { 
				SrmAbortFilesRequest request = new SrmAbortFilesRequest();
				request.setRequestToken(requestToken);
				org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs = new org.dcache.srm.v2_2.ArrayOfAnyURI();
				org.apache.axis.types.URI[] urlArray = new org.apache.axis.types.URI[1];
				urlArray[0] =  new URI((new GlobusURL(configuration.getSrmUrl())).getURL());
				arrayOfSURLs.setUrlArray(urlArray);
				request.setArrayOfSURLs(arrayOfSURLs);
				SrmAbortFilesResponse response = srm.srmAbortFiles(request);
				if (response==null) { 
					throw new IOException(" null SrmAbortFilesRespinse for request token " +requestToken);
				}
				TReturnStatus rs     = response.getReturnStatus();
				if ( rs == null) {
					throw new IOException(" null TReturnStatus for request token "+requestToken);
				}				
				if (RequestStatusTool.isFailedRequestStatus(rs)) {
					failed=true;
					sb.append("SrmAbortFiles failed for request token "+ requestToken +":\n ");
					sb.append("return status: "+rs.getStatusCode()+", Explanation : "+rs.getExplanation() +"\n");
				}
				if (response.getArrayOfFileStatuses()!=null) { 
					if (response.getArrayOfFileStatuses().getStatusArray()!=null) { 
						if (response.getArrayOfFileStatuses().getStatusArray().length>0) { 
							for(TSURLReturnStatus status: response.getArrayOfFileStatuses().getStatusArray()) { 
								TReturnStatus st = status.getStatus();
								if (st==null) { 
									sb.append(status.getSurl()+" TReturnStatus is null\n");
								}
								else { 
									sb.append(status.getSurl()+ " return code "+st.getStatusCode()+", Explanation "+st.getExplanation()+"\n");
								}
							}
						}
						else { 
							sb.append("TSURLReturnStatus is empty\n");
						}
					}
					else { 
						sb.append("TSURLReturnStatus is null\n");
					}
				}
				else { 
					sb.append("getArrayOfFileStatuses is null");
				}
			}
			if (failed) { 
				throw new IOException(sb.toString());
			}
		}
		else if (configuration.getSurls()!=null) { 
			SrmAbortFilesRequest request = new SrmAbortFilesRequest();
			org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs = new org.dcache.srm.v2_2.ArrayOfAnyURI();
			org.apache.axis.types.URI[] urlArray = new org.apache.axis.types.URI[configuration.getSurls().length];
			for(int i=0; i<configuration.getSurls().length;i++) { 
				urlArray[i] = new URI((new GlobusURL(configuration.getSurls()[i])).getURL());
			}
			arrayOfSURLs.setUrlArray(urlArray);
			request.setArrayOfSURLs(arrayOfSURLs);
			SrmAbortFilesResponse response =  srm.srmAbortFiles(request);
			if (response==null) { 
				throw new IOException(" null SrmAbortFilesResponse ");
			}
			TReturnStatus rs     = response.getReturnStatus();
			if ( rs == null) {
				throw new IOException(" null TReturnStatus ");
			}
			if (RequestStatusTool.isFailedRequestStatus(rs)) {
				sb.append("SrmAbortFiles failed:\n ");
				sb.append("return status: "+rs.getStatusCode()+", Explanation : "+rs.getExplanation() +"\n");
			}
			if (response.getArrayOfFileStatuses()!=null) { 
				if (response.getArrayOfFileStatuses().getStatusArray()!=null) { 
					if (response.getArrayOfFileStatuses().getStatusArray().length>0) { 
						for(TSURLReturnStatus status: response.getArrayOfFileStatuses().getStatusArray()) { 
							TReturnStatus st = status.getStatus();
							if (st==null) { 
								sb.append(status.getSurl()+" TReturnStatus is null\n");
							}
							else { 
								sb.append(status.getSurl()+ " return code "+st.getStatusCode()+", Explanation "+st.getExplanation()+"\n");
							}
						}
					}
					else { 
						sb.append("TSURLReturnStatus is empty\n");
					}
				}
				
				else { 
					sb.append("TSURLReturnStatus is null\n");
				}
			}
			else { 
				sb.append("getArrayOfFileStatuses is null");
			}
			if (RequestStatusTool.isFailedRequestStatus(rs)) {
				throw new IOException(sb.toString());
			}
		}
	}
}