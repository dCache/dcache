// $Id: CommandLineClient.java,v 1.4 2004-07-02 20:14:21 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3  2004/06/30 21:57:03  timur
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
 * @(#)CommandLineClient.java	0.9 05/27/02
 *
 * Copyright 2002 Fermi National Accelerator Lab. All rights reserved.
 * FNAL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package gov.fnal.srm.client;

import gov.fnal.srm.Manager;
import gov.fnal.srm.SRMException;
import gov.fnal.srm.util.ArgParser;
/**
 * CommanLineClient allows to execute various srm commands via
 * command line interface
 * CommandLineClient.java, Fri May 31 10:32:35 2002
 *
 * @author Timur Perelmutov
 * @author CD/ISD
 * @version 	0.9, 31 May 2002
 */

public class CommandLineClient {
    private static final int GET_COMMAND=0;
    private static final int RELEASE_COMMAND=1;
    private static final int PUT_COMMAND=2;
    private static final int PUT_DONE_COMMAND=3;
    private static final int COPY_COMMAND=4;
    private static final int TERMINATE_REQUEST_COMMAND=5;
    private static final int ABORT_FILE_COMMAND=6;
    private static final int CHANGE_FILE_STATUS_COMMAND=7;
    private static final int SUSPEND_REQUEST_COMMAND=8;
    private static final int RESUME_REQUEST_COMMAND=9;
    private static final int GET_REQUEST_STATUS_COMMAND=10;
    private static final int GET_FILES_STATUS_COMMAND=11;
    private static final int GET_REQUEST_SUMMARY_COMMAND=12;
    private static final int GET_FILES_METADATA_COMMAND=13;
    private static final int REQUEST_ESTIMATE_TIME_COMMAND=14;
    private static final int GET_PROTOCOLS_COMMAND=15;
    private static final int ADVISORY_DELETE_COMMAND=16;
    private static final int GET_REQUEST_ID_COMMAND=17;
    private static final int RENEW_LIFETIME_COMMAND=18;
    
    private static String commands[] = {
        "get",
        "release",
        "put",
        "putDone",
        "copy",
        "terminateRequest",
        "abortFile",
        "changeFileStatus",
        "suspendRequest",
        "resumeRequest",
        "getRequestStatus",
        "getFilesStatus",
        "getRequestSummary",
        "getFilesMetaData",
        "requestEstimateTime",
        "getProtocols",
        "advisoryDelete",
        "getRequestId",
        "renewLifetime"
    };
    
    private static String general_void_options[][] = {
        {"verbose","description"},
    };
    
    private static String general_string_options[][] = {
        {"srmhost","description"},
        {"srmpath","description"},
    };
    private static String general_int_options[][] = {
        {"srmport","description"},
    };
    private static int[][] general_int_options_limits = {
        {0,0xFFFF},
    };
    
    private static String[][][] command_void_options = {
        {},//"get",
        {},//"release",
        {},//"put",
        {},//"putDone",
        {},//"copy",
        {},//"terminateRequest",
        {},//"abortFile",
        {},//"changeFileStatus",
        {},//"suspendRequest",
        {},//"resumeRequest",
        {},//"getRequestStatus",
        {},//"getFilesStatus",
        {},//"getRequestSummary",
        {},//"getFilesMetaData",
        {},//"requestEstimateTime",
        {},//"getProtocols",
        {},//"advisoryDelete",
        {},//"getRequestId",
        {},//"renewLifetime"
    };
    
    private static String[][][] command_string_options = {
        {{"userID","description"},
         {"StorageUserID","description"},
         {"fileSpecification","description"},
         {"filesType","description"},
         {"RequestIDDescription","description"},
         {"protocol","description"},
        },//"get",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"release",
        {{"userID","description"},
         {"StorageUserID","description"},
         {"fileSpecification","description"},
         {"filesType","description"},
         {"RequestIDDescription","description"},
         {"protocol","description"},
        },//"put",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"putDone",
        {{"userID","description"},
         {"StorageUserID","description"},
         {"fileSpecification","description"},
         {"filesType","description"},
         {"RequestIDDescription","description"},
         {"protocol","description"},
        },//"copy",
        {{"userID","description"},
         {"requestID","description"},
        },//"terminateRequest",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"abortFile",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"changeFileStatus",
        {{"userID","description"},
         {"requestID","description"},
        },//"suspendRequest",
        {{"userID","description"},
         {"requestID","description"},
        },//"resumeRequest",
        {{"userID","description"},
         {"requestID","description"},
        },//"getRequestStatus",
        {{"userID","description"},
         {"requestID","description"},
        },//"getFilesStatus",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"getRequestSummary",
        {{"userID","description"},
         {"SURL","description"},
        },//"getFilesMetaData",
        {{"userID","description"},
         {"requestID","description"},
        },//"requestEstimateTime",
        {{"userID","description"},
        },//"getProtocols",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"advisoryDelete",
        {{"userID","description"},
         {"requestIDDescription","description"},
        },//"getRequestId",
        {{"userID","description"},
         {"requestID","description"},
         {"SURL","description"},
        },//"renewLifetime"
    };
    
    private static String[][][] command_int_options = {
        {{"Timeout","description"}},//"get",
        {},//"release",
        {},//"put",
        {},//"putDone",
        {},//"copy",
        {},//"terminateRequest",
        {},//"abortFile",
        {},//"changeFileStatus",
        {},//"suspendRequest",
        {},//"resumeRequest",
        {},//"getRequestStatus",
        {},//"getFilesStatus",
        {},//"getRequestSummary",
        {},//"getFilesMetaData",
        {},//"requestEstimateTime",
        {},//"getProtocols",
        {},//"advisoryDelete",
        {},//"getRequestId",
        {},//"renewLifetime"
    };
    
    private static int[][][] command_int_options_limits = {
        {{0,Integer.MAX_VALUE}/*"Timeout"*/},//"get",
        {},//"release",
        {},//"put",
        {},//"putDone",
        {},//"copy",
        {},//"terminateRequest",
        {},//"abortFile",
        {},//"changeFileStatus",
        {},//"suspendRequest",
        {},//"resumeRequest",
        {},//"getRequestStatus",
        {},//"getFilesStatus",
        {},//"getRequestSummary",
        {},//"getFilesMetaData",
        {},//"requestEstimateTime",
        {},//"getProtocols",
        {},//"advisoryDelete",
        {},//"getRequestId",
        {},//"renewLifetime"
    };
    
    private CommandLineClient() {
    }
    
    private static ArgParser parser;
    
    private static void PrepareParser(String[] argv) throws SRMException {
        parser = new ArgParser(argv,commands);
        for(int i =0 ; i<general_void_options.length;++i) {
            parser.addVoidOption(null,general_void_options[i][0], general_void_options[i][1]);
        }
        for(int i =0 ; i<general_string_options.length;++i) {
            parser.addStringOption(null,general_string_options[i][0], general_string_options[i][1]);
        }
        
        for(int i =0 ; i<general_int_options.length;++i) {
            parser.addIntegerOption(null,general_int_options[i][0], general_int_options[i][1],
            general_int_options_limits[i][0],general_int_options_limits[i][1]);
        }
        
        for(int j=0; j<commands.length;++j) {
            for(int i =0 ; i<command_void_options[j].length;++i) {
                parser.addVoidOption(commands[j],command_void_options[j][i][0],
                command_void_options[j][i][1]);
            }
            for(int i =0 ; i<command_string_options[j].length;++i) {
                parser.addStringOption(commands[j],command_string_options[j][i][0],
                command_string_options[j][i][1]);
            }
            
            for(int i =0 ; i<command_int_options[j].length;++i) {
                parser.addIntegerOption(commands[j],command_int_options[j][i][0],
                command_int_options[j][i][1], command_int_options_limits[j][i][0],
                command_int_options_limits[j][i][1]);
            }
        }
    }
    
    private static String executeGetCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String storageUserID=null;
        if(parser.isOptionSet(command,"storageUserID")) {
            storageUserID = parser.stringOptionValue(command,"storageUserID");
        }
        
        int timeout = 0;
        if(parser.isOptionSet(command,"timeout")) {
            timeout = parser.intOptionValue(command,"timeout");
        }
        
        String fileSpecification;
        if(parser.isOptionSet(command,"fileSpecification")) {
            fileSpecification = parser.stringOptionValue(command,"fileSpecification");
        }
        else {
            throw new SRMException("for command "+command+" -fileSpecification option is required");
        }
        
        String filesType = null;
        if(parser.isOptionSet(command,"filesType")) {
            filesType = parser.stringOptionValue(command,"filesType");
        }
        
        String requestIDDescription = null;
        if(parser.isOptionSet(command,"requestIDDescription")) {
            requestIDDescription = parser.stringOptionValue(command,"requestIDDescription");
        }
        
        String protocol = null;
        if(parser.isOptionSet(command,"protocol")) {
            protocol = parser.stringOptionValue(command,"protocol");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGet(userID,storageUserID,timeout,fileSpecification,
        filesType,requestIDDescription,protocol);
        
    }
    
    private static String executeReleseCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmRelease(userID,requestID,SURL);
        
    }
    
    private static String executePutCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String storageUserID=null;
        if(parser.isOptionSet(command,"storageUserID")) {
            storageUserID = parser.stringOptionValue(command,"storageUserID");
        }
        
        
        String fileSpecification;
        if(parser.isOptionSet(command,"fileSpecification")) {
            fileSpecification = parser.stringOptionValue(command,"fileSpecification");
        }
        else {
            throw new SRMException("for command "+command+" -fileSpecification option is required");
        }
        
        String filesType = null;
        if(parser.isOptionSet(command,"filesType")) {
            filesType = parser.stringOptionValue(command,"filesType");
        }
        
        String requestIDDescription;
        if(parser.isOptionSet(command,"requestIDDescription")) {
            requestIDDescription = parser.stringOptionValue(command,"requestIDDescription");
        }
        else {
            throw new SRMException("for command "+command+" -requestIDDescription option is required");
        }
        
        String protocol = null;
        if(parser.isOptionSet(command,"protocol")) {
            protocol = parser.stringOptionValue(command,"protocol");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmPut(userID,storageUserID,fileSpecification,
        filesType,requestIDDescription,protocol);
        
    }
    
    private static String executePutDoneCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL = null;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmPutDone(userID,requestID,SURL);
        
    }
    
    private static String executeCopyCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String storageUserID=null;
        if(parser.isOptionSet(command,"storageUserID")) {
            storageUserID = parser.stringOptionValue(command,"storageUserID");
        }
        
        
        String fileSpecification;
        if(parser.isOptionSet(command,"fileSpecification")) {
            fileSpecification = parser.stringOptionValue(command,"fileSpecification");
        }
        else {
            throw new SRMException("for command "+command+" -fileSpecification option is required");
        }
        
        String filesType = null;
        if(parser.isOptionSet(command,"filesType")) {
            filesType = parser.stringOptionValue(command,"filesType");
        }
        
        String requestIDDescription ;
        if(parser.isOptionSet(command,"requestIDDescription")) {
            requestIDDescription = parser.stringOptionValue(command,"requestIDDescription");
        }
        else {
            throw new SRMException("for command "+command+" -requestIDDescription option is required");
        }
        
        String protocol = null;
        if(parser.isOptionSet(command,"protocol")) {
            protocol = parser.stringOptionValue(command,"protocol");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmCopy(userID,storageUserID,fileSpecification,
        filesType,requestIDDescription,protocol);
        
    }
    
    private static String executeTerminateRequestCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        
        Manager manager = connectToManager(parser);
        
        return manager.srmTerminateRequest(userID,requestID);
        
    }
    
    private static String executeAbortFileCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmAbortFile(userID,requestID,SURL);
        
    }
    
    private static String executeChangeFileStatusCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        String filesType;
        if(parser.isOptionSet(command,"filesType")) {
            filesType = parser.stringOptionValue(command,"filesType");
        }
        else {
            throw new SRMException("for command "+command+" -filesType option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmChangeFileStatus(userID,requestID,SURL,filesType);
    }
    
    private static String executeSuspendRequestCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        
        Manager manager = connectToManager(parser);
        
        return manager.srmSuspendRequest(userID,requestID);
        
    }
    
    private static String executeResumeRequestCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmResumeRequest(userID,requestID);
        
    }
    
    private static String executeGetRequestStatusCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetRequestStatus(userID,requestID);
        
    }
    
    private static String executeGetFilesStatusCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetFilesStatus(userID,requestID,SURL);
        
    }
    
    private static String executeGetRequestSummaryCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetRequestSummary(userID,requestID);
        
    }
    
    private static String executeGetFilesMetaDataCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetFilesMetaData(userID,SURL);
    }
    
    private static String executeRequestEstimateTimeCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmRequestEstimateTime(userID,requestID);
        
    }
    
    private static String executeGetProtocolsCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetProtocols(userID);
        
    }
    
    private static String executeAdvisoryDeleteCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmAdvisoryDelete(userID,requestID,SURL);
        
    }
    
    private static String executeGetRequestIDCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestIDDescription;
        if(parser.isOptionSet(command,"requestIDDescription")) {
            requestIDDescription = parser.stringOptionValue(command,"requestIDDescription");
        }
        else {
            throw new SRMException("for command "+command+" -requestIDDescription option is required");
        }
        
        
        
        Manager manager = connectToManager(parser);
        
        return manager.srmGetRequestID(userID,requestIDDescription);
        
    }
    
    private static String executeRenewLifetimeCommand(String command,ArgParser parser) throws SRMException {
        String userID;
        if(parser.isOptionSet(command,"userID")) {
            userID = parser.stringOptionValue(command,"userID");
        }
        else {
            throw new SRMException("for command "+command+" -userID option is required");
        }
        
        String requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.stringOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        
        String SURL;
        if(parser.isOptionSet(command,"SURL")) {
            SURL = parser.stringOptionValue(command,"SURL");
        }
        else {
            throw new SRMException("for command "+command+" -SURL option is required");
        }
        
        Manager manager = connectToManager(parser);
        
        return manager.srmRenewLifetime(userID,requestID,SURL);
        
    }
    
    /**
     * Main </p>
     *
     * @param  argv
     *         contains command line arguments
     * @return return value description
     *
     * @throws
     *
     */
    private static String executeCommand(ArgParser parser) throws SRMException {
        String command = parser.getCommand();
        int i;
        for(i = 0;i<commands.length;++i) {
            if(commands[i].equals(command)) {
                break;
            }
        }
        
        switch(i) {
            case GET_COMMAND: {
                return executeGetCommand(command,parser);
            }
            case RELEASE_COMMAND: {
                return executeReleseCommand(command,parser);
            }
            case PUT_COMMAND: {
                return executePutCommand(command,parser);
            }
            case PUT_DONE_COMMAND: {
                return executePutDoneCommand(command,parser);
            }
            case COPY_COMMAND: {
                return executeCopyCommand(command,parser);
            }
            case TERMINATE_REQUEST_COMMAND: {
                return executeTerminateRequestCommand(command,parser);
            }
            case ABORT_FILE_COMMAND: {
                return executeAbortFileCommand(command,parser);
            }
            case CHANGE_FILE_STATUS_COMMAND: {
                return executeChangeFileStatusCommand(command,parser);
            }
            case SUSPEND_REQUEST_COMMAND: {
                return executeSuspendRequestCommand(command,parser);
            }
            case RESUME_REQUEST_COMMAND: {
                return executeResumeRequestCommand(command,parser);
            }
            case GET_REQUEST_STATUS_COMMAND: {
                return executeGetRequestStatusCommand(command,parser);
            }
            case GET_FILES_STATUS_COMMAND: {
                return executeGetFilesStatusCommand(command,parser);
            }
            case GET_REQUEST_SUMMARY_COMMAND: {
                return executeGetRequestSummaryCommand(command,parser);
            }
            case GET_FILES_METADATA_COMMAND: {
                return executeGetFilesMetaDataCommand(command,parser);
            }
            case REQUEST_ESTIMATE_TIME_COMMAND: {
                return executeRequestEstimateTimeCommand(command,parser);
            }
            case GET_PROTOCOLS_COMMAND: {
                return executeGetProtocolsCommand(command,parser);
            }
            case ADVISORY_DELETE_COMMAND: {
                return executeAdvisoryDeleteCommand(command,parser);
            }
            case GET_REQUEST_ID_COMMAND: {
                return executeGetRequestIDCommand(command,parser);
            }
            case RENEW_LIFETIME_COMMAND: {
                return executeRenewLifetimeCommand(command,parser);
            }
            default: {
                throw new SRMException("unrecognized command:"+command);
            }
        }
    }
    
    /**
     * Manager </p>
     *
     * @param  parser
     *         contains parsed command line arguments
     * @return Manager interface
     *
     * @throws
     *
     */
    private static Manager connectToManager(ArgParser parser) throws SRMException {
        String host;
        if( parser.isOptionSet(null,"srmhost")) {
            host = parser.stringOptionValue(null,"srmhost");
        }
        else {
            host = "localhost";
        }
        
        short port = Client.DEFAULTPORT;
        if(parser.isOptionSet(null,"srmport")) {
            port = (short)parser.intOptionValue(null,"srmport");
        }
        
        String srmpath =Client.DEFAULTPATH;
        if(parser.isOptionSet(null,"srmpath")) {
            srmpath = parser.stringOptionValue(null,"srmpath");
        }
        
        Client client = new Client(host,port,srmpath);
        
        Manager manager =  client.getManagerConnection();
        if(manager == null) {
            throw new SRMException("can not get manager connection");
        }
        return manager;
    }
    
    /**
     * Main </p>
     *
     * @param  argv
     *         contains command line arguments
     * @return return value description
     *
     * @throws
     *
     */
    
    public static void main(String[] argv) throws Exception {
        boolean usage = false;
        boolean command_usage = false;
        
        if(argv.length >1 && argv[0].equals("--help-command")) {
            command_usage = true;
        }
        
        for(int i=0;i<argv.length;++i) {
            if(argv[i].equalsIgnoreCase("-h") || argv[i].equalsIgnoreCase("-help") ||
            argv[i].equalsIgnoreCase("--h") || argv[i].equalsIgnoreCase("--help") ) {
                usage = true;
            }
        }
        System.out.println();
        try {
            PrepareParser(argv) ;
        }
        catch(SRMException srme) {
            srme.printStackTrace();
            return;
        }
        
        if(command_usage) {
            System.out.println(parser.usage(argv[1]));
            return;
        }
        
        if(usage) {
            System.out.println(parser.usage());
            return;
        }
        
        try {
            parser.parse();
        }
        catch(SRMException srme) {
            System.err.println("failed to parse command line:\n"+srme.getMessage());
            System.err.println("srm -h for usage");
            return;
        }
        //System.out.println("parse succeded");
        
        try {
            System.out.println(executeCommand(parser));
        }
        catch(SRMException srme) {
            System.err.println("failed to execute command :"+srme.getMessage());
            System.err.println("srm -h for usage");
            return;
        }
        
    }
}
