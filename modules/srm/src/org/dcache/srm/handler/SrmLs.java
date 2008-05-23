/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMTooManyResultsException;



import java.text.DateFormat;


/**
 *
 * @author  timur
 */
public class SrmLs {
    private final static String SFN_STRING="?SFN=";
    private int maxNumOfLevels ;
    AbstractStorageElement storage;
    SrmLsRequest request;
    SrmLsResponse response;
    RequestUser user;
    private int results_num;
    private int max_results_num;
    int numOfLevels =1;

    /** Creates a new instance of SrmLs */
    public SrmLs(RequestUser user,
		 RequestCredential credential,
		 SrmLsRequest request,
		 AbstractStorageElement storage,
		 org.dcache.srm.SRM srm,
		 String client_host) {
        this(user,request,storage,1000);
    }

    public SrmLs(RequestUser user,
		 SrmLsRequest request, 
		 AbstractStorageElement storage,
		 int max_results_num) {
        this(user,request,storage,max_results_num,100);
    }
    
    public SrmLs(RequestUser user,SrmLsRequest request, 
		 AbstractStorageElement storage,
		 int max_results_num,int maxNumOfLevels) {
        this.request = request;
        this.user    = user;
        this.storage = storage;
        this.max_results_num = max_results_num;
        this.maxNumOfLevels  = maxNumOfLevels;
    }
    
    public static final SrmLsResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final  SrmLsResponse getFailedResponse(String error,
							 TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmLsResponse response = new SrmLsResponse();
        response.setReturnStatus(status);
        return response;
    }

    private boolean increaseResultsNumAndContinue(){
        if(results_num >=max_results_num) {
            return false;
        }
        results_num++;
        return true;
    }
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmLs "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmLs "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmLs exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmLsResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmLs();
        } catch(Exception e) {
            storage.elog(e);
            response = new SrmLsResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            response.setReturnStatus(returnStatus);
        }
        
        return response;
    }
    /**
     * implementation of srm ls
     */
    public SrmLsResponse srmLs()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException{
        
        
        say("Entering srmLs");
        
        SrmLsResponse srmLsResponse = new SrmLsResponse();
        
        // The SRM specification is not clear, but
        // probably intends that zero (0) means "no
        // recursion", one (1) means "current
        // directory plus one (1) level down, et
        // cetera.
        // Internally, we'll set this value to -1
        // to indicate "no limit".
        
        if (request.getAllLevelRecursive() != null &&
	    request.getAllLevelRecursive().booleanValue()) {
            numOfLevels= maxNumOfLevels;
        }
	else {
            if(request.getNumOfLevels() !=null) {
                numOfLevels = request.getNumOfLevels().intValue();
                // The spec doesn't say what to do in case of negative
                // values, so filter 'em out...
                
                if (numOfLevels < 0) {
                    return getFailedResponse("numOfLevels < 0",
					     TStatusCode.SRM_INVALID_REQUEST);
                }
            } 
	    else {
                numOfLevels = 1;
            }
        }

	int offset =  request.getOffset() !=null ? request.getOffset().intValue() : 0;
	int count  =  request.getCount() !=null ? request.getCount().intValue() : 0;

	if (numOfLevels>1 && (offset > 0 || count > 0)) {
	    return getFailedResponse("numOfLevels>1 together with offset and/or count is not supported",
				     TStatusCode.SRM_INVALID_REQUEST);
	}
	if (offset<0) { 
	    return getFailedResponse(" offset value less than 0, disallowed ",
				     TStatusCode.SRM_INVALID_REQUEST);
	}

	if (count<0) { 
            return getFailedResponse(" count value less than 0, disallowed",
				     TStatusCode.SRM_INVALID_REQUEST);
        }
        
        // SrmLsResponse consists of two parts - a TReturnStatus
        // and a ArrayOfTMetaDataPathDetail.
        
        // First set the TReturnStatus
        TReturnStatus returnStatus = new TReturnStatus();
        returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        returnStatus.setExplanation("srm-ls completed normally");
        srmLsResponse.setReturnStatus(returnStatus);
        // Now get the information from dCache and fill in
        // the above stub.
        
        if(request.getFullDetailedList() != null) {
            longFormat = request.getFullDetailedList().booleanValue();
        }
        if( request.getArrayOfSURLs() == null) {
            return getFailedResponse(" null Path array",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        org.apache.axis.types.URI [] surlInfos = request.getArrayOfSURLs().getUrlArray();
	
	if (request.getOffset()!=null) { 
	    if (request.getOffset().intValue()<0) { 
		return getFailedResponse(" offset value less than 0, diallowed ",
					 TStatusCode.SRM_INVALID_REQUEST);
	    }
	}

        
        TMetaDataPathDetail[] metaDataPathDetails =
                new TMetaDataPathDetail[surlInfos.length];
        
        // Now, iterate one by one through the URIs/filespecs
        // passed in from the request.
        for (int i = 0; i < surlInfos.length; i++) {
            
            org.apache.axis.types.URI surl = surlInfos[i];
            
            say("SURL["+i+"]="+surl);
            port = surl.getPort();
            host = surl.getHost();
            
            String path = surl.getPath(true,true);
            int indx=path.indexOf(SFN_STRING);
            if( indx != -1) {
                servicePathAndSFNPart = path.substring(0,indx+SFN_STRING.length());
                path=path.substring(indx+SFN_STRING.length());
            }
            
            
            say("Path: " + path);
            // List itemlist = fspath.getPathItemsList();

            TMetaDataPathDetail metaDataPathDetail=null;
            try 
	    { 
		metaDataPathDetail =
		    getMetaDataPathDetail( path,
					   0,
					   offset, 
					   count, 
					   null);
	    }
	    catch (SRMException srme) 
	    { 	

		TReturnStatus status=null;
		    
		
		if (srme instanceof SRMInternalErrorException) { 
		    status = new TReturnStatus(TStatusCode.SRM_FAILURE, srme.getMessage());
		    srmLsResponse.getReturnStatus().setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
		    srmLsResponse.getReturnStatus().setExplanation(srme.getMessage());
		}
		else if ( srme instanceof SRMTooManyResultsException) { 
		    status = new TReturnStatus(TStatusCode.SRM_FAILURE, srme.getMessage());
		    srmLsResponse.getReturnStatus().setStatusCode(TStatusCode.SRM_TOO_MANY_RESULTS);
		    srmLsResponse.getReturnStatus().setExplanation(srme.getMessage());
		}
		else { 
		    status = new TReturnStatus(TStatusCode.SRM_INVALID_PATH, srme.getMessage());
		    srmLsResponse.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
		    srmLsResponse.getReturnStatus().setExplanation("path does not exist for one or more files specified, check individual statuses \n");
		}

		metaDataPathDetail =  new TMetaDataPathDetail(path,
							      status,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null,
							      null);

	    }
	    finally
	    {
		metaDataPathDetails[i] = metaDataPathDetail;
	    }
        }
        srmLsResponse.setDetails(new ArrayOfTMetaDataPathDetail(metaDataPathDetails));
        return srmLsResponse;
        
        
    }
    
    
    public TMetaDataPathDetail getMetaDataPathDetail(
            String path,
            int depth,
	    int offset,
	    int count,
            FileMetaData parent_fmd)
	throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        if(!increaseResultsNumAndContinue()) {
            throw new SRMTooManyResultsException("max results number of "+max_results_num+" exceeded. Try to narrow down with offset and count \n");
        }
        FileMetaData fmd = storage.getFileMetaData(user, path,parent_fmd);
        if(!canRead(user,fmd)) {
            return null;
        }
        TMetaDataPathDetail metaDataPathDetail =
                new TMetaDataPathDetail();
        metaDataPathDetail.setLifetimeAssigned(new Integer(-1));
        metaDataPathDetail.setLifetimeLeft(new Integer(-1));
        TUserPermission userPermission = new TUserPermission();
        userPermission.setUserID(fmd.owner);
        TPermissionMode permissionMode;
        int userPerm = (fmd.permMode >> 6) & 7;
        userPermission.setMode(maskToTPermissionMode(userPerm));
        metaDataPathDetail.setOwnerPermission(userPermission);
	TGroupPermission groupPermission = new TGroupPermission();
	groupPermission.setGroupID(fmd.group);
	int groupPerm = (fmd.permMode >> 3) & 7;
	groupPermission.setMode(maskToTPermissionMode(groupPerm));
	metaDataPathDetail.setGroupPermission(groupPermission);
	metaDataPathDetail.setOtherPermission(maskToTPermissionMode(fmd.permMode & 7));
        org.apache.axis.types.URI turi =
	    new org.apache.axis.types.URI();
        turi.setScheme("srm");
        metaDataPathDetail.setPath(path);
        // creation time
        java.util.GregorianCalendar td =
                new java.util.GregorianCalendar();
        td.setTimeInMillis(fmd.creationTime);
        metaDataPathDetail.setCreatedAtTime(td);
        //modification time
        td = new java.util.GregorianCalendar();
        td.setTimeInMillis(fmd.lastModificationTime);
        metaDataPathDetail.setLastModificationTime(td);
        if(fmd.checksumType != null && fmd.checksumValue != null ) {
            metaDataPathDetail.setCheckSumType(fmd.checksumType);
            metaDataPathDetail.setCheckSumValue(fmd.checksumValue);
        }
        metaDataPathDetail.setFileStorageType(TFileStorageType.PERMANENT);
	if (!fmd.isPermanent) { 
	    if (fmd.isPinned) { 
		metaDataPathDetail.setFileStorageType(TFileStorageType.DURABLE);
	    }
	    else {
		metaDataPathDetail.setFileStorageType(TFileStorageType.VOLATILE);
	    }
	}
        if(fmd.isDirectory) {
            metaDataPathDetail.setType(TFileType.DIRECTORY);
        } 
	else if(fmd.isLink) {
            metaDataPathDetail.setType(TFileType.LINK);
        } 
	else if(fmd.isRegular) {
            metaDataPathDetail.setType(TFileType.FILE);
        } 
	else {
            say("file type is Unknown");
        }
	TFileLocality fileLocality = TFileLocality.NONE;
	if (fmd.isCached) { 
	    if (fmd.isStored) { 
		fileLocality = TFileLocality.ONLINE_AND_NEARLINE;
	    }
	    else { 
		fileLocality = TFileLocality.ONLINE;
	    }
	}
	else { 
	    fileLocality = TFileLocality.NEARLINE;
	}
	if (fmd.isDirectory) {
		fileLocality = TFileLocality.NONE;	
	}
	metaDataPathDetail.setFileLocality(fileLocality);
	if (fmd.retentionPolicyInfo!=null) { 
	    metaDataPathDetail.setRetentionPolicyInfo(new TRetentionPolicyInfo(fmd.retentionPolicyInfo.getRetentionPolicy(),
									       fmd.retentionPolicyInfo.getAccessLatency()));
	}
        metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(fmd.size));
	if (fmd.spaceTokens!=null) { 
	    if (fmd.spaceTokens.length > 0) { 
		ArrayOfString arrayOfSpaceTokens = new ArrayOfString(new String[fmd.spaceTokens.length]);
		for (int st=0;st<fmd.spaceTokens.length;st++) {
		    StringBuffer spaceToken = new StringBuffer();
		    spaceToken.append(fmd.spaceTokens[st]);
		    arrayOfSpaceTokens.setStringArray(st,spaceToken.toString());
		}
		metaDataPathDetail.setArrayOfSpaceTokens(arrayOfSpaceTokens);
	    }
	}
        TReturnStatus returnStatus = new TReturnStatus();
        returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        metaDataPathDetail.setStatus(returnStatus);
	
        say("depth = "+depth+" and numOfLevels = "+numOfLevels);
        if (metaDataPathDetail.getType() == TFileType.DIRECTORY && depth<numOfLevels ) {
            say("depth < numOfLevels => get listing for this directory");
                java.io.File dirFiles[] = storage.listDirectoryFiles(user,path,fmd);
		TMetaDataPathDetail dirMetaDataPathDetails[]=null;
		if(dirFiles != null && dirFiles.length >0) {
		    int end   = dirFiles.length;
		    int start = offset;
		    if ( count != 0 &&  offset + count <= dirFiles.length) { 
			    end = offset + count;
		    }
		    int len = end - start;
		    if ( offset <  dirFiles.length ) { 
			dirMetaDataPathDetails = new TMetaDataPathDetail[len];
			for (int j = start; j< end; j++) {
				String subpath = path+'/'+dirFiles[j].getName();
				try {
					TMetaDataPathDetail dirMetaDataPathDetail;
					if (longFormat) { 
						dirMetaDataPathDetail = getMetaDataPathDetail(subpath, depth+1,offset,count,fmd);
					}
					else { 
						if((depth+1>=numOfLevels)||dirFiles[j].isFile()) {
							dirMetaDataPathDetail =  getMinimalMetaDataPathDetail(subpath,dirFiles[j]);
						} 
						else {
							dirMetaDataPathDetail = getMetaDataPathDetail(subpath, depth+1,offset,count,fmd);
						}
					}
					dirMetaDataPathDetails[j-start] = dirMetaDataPathDetail;
				}
				catch (SRMException srme) {
					dirMetaDataPathDetails[j-start] = null;
				}
			}
		    }
		}
		metaDataPathDetail.setArrayOfSubPaths(new ArrayOfTMetaDataPathDetail(dirMetaDataPathDetails));
	}
	return metaDataPathDetail;
    }
    
    public TMetaDataPathDetail getMinimalMetaDataPathDetail(
            String path,
            java.io.File file
            )
            throws SRMException,org.apache.axis.types.URI.MalformedURIException {
        if(!increaseResultsNumAndContinue()) {
            throw new SRMTooManyResultsException("max results number of "+max_results_num+" exceeded. Try to be narrow down with offset and count");
        }
        
        TMetaDataPathDetail metaDataPathDetail =
                new TMetaDataPathDetail();
        metaDataPathDetail.setLifetimeAssigned(new Integer(-1));
        metaDataPathDetail.setLifetimeLeft(new Integer(-1));
        
        org.apache.axis.types.URI turi =
                new org.apache.axis.types.URI();
        // new org.apache.axis.types.URI(inPath);
        
        turi.setScheme("srm");
        
        // To do:  replace the below dummy values
        //turi.setHost(host);
        //turi.setPort(port);
        
        //turi.setPath(servicePathAndSFNPart+path);
        
        //org.apache.axis.types.URI tsurl = new org.apache.axis.types.URI(turi);
        metaDataPathDetail.setPath(path);
        
        
        java.util.GregorianCalendar td =
                new java.util.GregorianCalendar();
        td.setTimeInMillis(file.lastModified());
        metaDataPathDetail.setCreatedAtTime(td);
        metaDataPathDetail.setLastModificationTime(td);
        
        
        metaDataPathDetail.setFileStorageType(TFileStorageType.PERMANENT);
        if(file.isDirectory()) {
            metaDataPathDetail.setType(TFileType.DIRECTORY);
        } else if(file.isFile()) {
            metaDataPathDetail.setType(TFileType.FILE);
        } else {
            say("file type is Unknown");
        }

	if (file.length()==1) {
		FileMetaData fmd = storage.getFileMetaData(user, path, null);
		 metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(fmd.size));
	}
	else { 
		metaDataPathDetail.setSize(new org.apache.axis.types.UnsignedLong(file.length()));
	}
        TReturnStatus returnStatus = new TReturnStatus();
        returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        metaDataPathDetail.setStatus(returnStatus);
        return metaDataPathDetail;
    }
    
    public boolean canRead(RequestUser user, FileMetaData fmd) {
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;
        
        if(permissions == 0 ) {
            return false;
        }
        
        if(Permissions.worldCanRead(permissions)) {
            return true;
        }
        
        if(uid == -1 || gid == -1) {
            return false;
        }
        
        if(user == null ) {
            return false;
        }
        
        if(user.getGid() == gid && Permissions.groupCanRead(permissions)) {
            return true;
        }
        
        if(user.getUid() == uid && Permissions.userCanRead(permissions)) {
            return true;
        }
        
        return false;
    }
    
    public TPermissionMode maskToTPermissionMode(int permMask) {
        switch(permMask) {
            case 0: return TPermissionMode.NONE;
            case 1: return TPermissionMode.X;
            case 2: return TPermissionMode.W;
            case 3: return TPermissionMode.WX;
            case 4: return TPermissionMode.R;
            case 5: return TPermissionMode.RX;
            case 6: return TPermissionMode.RW;
            case 7: return TPermissionMode.RWX;
            default:
                throw new IllegalArgumentException("illegal perm mask: "+permMask);
        }
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
