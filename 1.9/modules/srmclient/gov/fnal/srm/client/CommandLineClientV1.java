// $Id: CommandLineClientV1.java,v 1.8 2004-07-02 20:14:21 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.7  2004/06/30 21:57:04  timur
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

import gov.fnal.srm.ISRM;
import gov.fnal.srm.RequestStatus;
import gov.fnal.srm.RequestFileStatus;
import gov.fnal.srm.FileMetaData;
import gov.fnal.srm.SRMException;
import gov.fnal.srm.util.ArgParser;

import gov.fnal.srm.security.GSSSocketFactory;
import gov.fnal.srm.security.GSSCipherPlugin;
import gov.fnal.srm.security.GssGsiCipherPlugin;
import gov.fnal.srm.security.SslGsiSocketFactory;

import electric.net.socket.SocketFactories;
import electric.net.socket.ISocketFactory;

/**
 * CommanLineClient allows to execute various srm commands via
 * command line interface
 * CommandLineClient.java, Fri May 31 10:32:35 2002
 *
 * @author Timur Perelmutov
 * @author CD/ISD
 * @version 	0.9, 31 May 2002
 */

public class CommandLineClientV1 {
    private static final int GET_COMMAND=0;
    private static final int PUT_COMMAND=1;
    private static final int MK_PERMANENT_COMMAND=2;
    private static final int PIN_COMMAND=3;
    private static final int UNPIN_COMMAND=4;
    private static final int GET_REQUEST_STATUS_COMMAND=5;
    private static final int GET_FILE_METADATA_COMMAND=6;
    private static final int GET_PROTOCOLS_COMMAND=7;
    private static final int GET_EST_GET_TIME=8;
    private static final int GET_EST_PUT_TIME=9;
    private static final int SET_FILE_STATUS_COMMAND=10;
    private static final int ADVISORY_DELETE_COMMAND=11;
    
    
    private static String commands[] = {
        "get",
        "put",
        "mkPermanent",
        "pin",
        "unpin",
        "getRequestStatus",
        "getFileMetaData",
        "getProtocols",
        "getEstGetTime",
        "getEstPutTime",
        "setFileStatus",
        "advisoryDelete"
    };
    
    private static String general_void_options[][] = {
        {"verbose","description"},
        {"gsissl","description"},
        {"gsiauth","description"},
    };
    
    private static String general_string_options[][] = {
        {"srmhost","description"},
        {"mapfile","description"},
        {"srmpath","description"},
        {"glueprotocol","description"},
    };
    private static String general_int_options[][] = {
        {"srmport","description"},
    };
    private static int[][] general_int_options_limits = {
        {0,0xFFFF},
    };
    
    private static String[][][] command_void_options = {
        {},//"get",
        {},//"put",
        {},//"mkPermanent",
        {},//"pin",
        {},//"unpin",
        {},//"getRequestStatus",
        {},//"getFileMetaData",
        {},//"getProtocols",
        {},//"getEstGetTime",
        {},//"getEstPutTime",
        {},//"setFileStatus",
        {},//"advisoryDelete",
    };
    
    private static String[][][] command_string_options = {
        {{"SURLS","description"},
         {"Protocols","description"},
        },//"get",
        {{"srcs","comma separated list of sourses"},
         {"dsts","comma separated list of destinations"},
         {"sizes","comma separated list of positive integers (sizes)"},
         {"permanent","comma separated list of boolean values \n"+
          " \t\t\"t\" or \"true\" is considered to be true, anything else is false"},
          {"protocols","comma separated list of protocols"},
        },//"put",
        {{"SURLS","description"},
        },//"mkPermanent",
        {{"TURLS","description"},
        },//"pin",
        {{"TURLS","description"},
         // {"RequestID","description"},
        },//"unpin",
        {},//"getRequestStatus",
        {{"SURLS","description"},
        },//"getFileMetaData",
        {
        },//"getProtocols",
        {{"SURLS","comma separated list of SURLS"},
         {"protocols","comma separated list of protocols"},
        },//"getEstGetTime",
        {{"srcs","comma separated list of sourses"},
         {"dsts","comma separated list of destinations"},
         {"sizes","comma separated list of positive integers (sizes)"},
         {"permanent","comma separated list of boolean values \n"+
          " \t\t\"t\" or \"true\" is considered to be true, anything else is false"},
          {"protocols","comma separated list of protocols"},
        },//"getEstPutTime",
        {
            
            {"state","description"},
        },//"setFileStatus",
        {{"SURLS","description"},
        },//"advisoryDelete",
    };
    
    private static String[][][] command_int_options = {
        {},//"get",
        {},//"put",
        {},//"mkPermanent",
        {},//"pin",
        {{"requestID","description"},},//"unpin",
        {{"requestID","description"},},//"getRequestStatus",
        {},//"getFileMetaData",
        {},//"getProtocols",
        {},//"getEstGetTime",
        {},//"getEstPutTime",
        {{"requestID","description"},
         {"fileID","description"},},//"setFileStatus",
         {},//"advisoryDelete",
    };
    
    private static int[][][] command_int_options_limits = {
        {},//"get",
        {},//"put",
        {},//"mkPermanent",
        {},//"pin",
        {{Integer.MIN_VALUE,Integer.MAX_VALUE}},//"unpin",
        {{Integer.MIN_VALUE,Integer.MAX_VALUE}},//"getRequestStatus",
        {},//"getFileMetaData",
        {},//"getProtocols",
        {},//"getEstGetTime",
        {},//"getEstPutTime",
        {{Integer.MIN_VALUE,Integer.MAX_VALUE},
         {Integer.MIN_VALUE,Integer.MAX_VALUE},},//"setFileStatus",
         {},//"advisoryDelete",
    };
    
    private CommandLineClientV1() {
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
    
    private static String executeGetCommand(String command,ArgParser parser)
    throws SRMException {
        String surls;
        if(parser.isOptionSet(command,"SURLS")) {
            surls = parser.stringOptionValue(command,"SURLS");
        }
        else {
            throw new SRMException("for command "+command+" -SURLS option is required");
        }
        System.out.println("surls ="+surls);
        String protocols;
        if(parser.isOptionSet(command,"Protocols")) {
            protocols = parser.stringOptionValue(command,"Protocols");
        }
        else {
            throw new SRMException("for command "+command+" -Protocols option is required");
        }
        
        String[] surls_arr = strToArray(surls);
        String[] protocols_arr = strToArray(protocols);
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.get(surls_arr,protocols_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
        
    }
    
    
    private static String executePutCommand(String command,ArgParser parser) throws SRMException {
        String srcs;
        if(parser.isOptionSet(command,"srcs")) {
            srcs = parser.stringOptionValue(command,"srcs");
        }
        else {
            throw new SRMException("for command "+command+" -srcs option is required");
        }
        System.out.println("srcs ="+srcs);
        String dsts;
        if(parser.isOptionSet(command,"dsts")) {
            dsts = parser.stringOptionValue(command,"dsts");
        }
        else {
            throw new SRMException("for command "+command+" -dsts option is required");
        }
        String sizes;
        if(parser.isOptionSet(command,"sizes")) {
            sizes = parser.stringOptionValue(command,"sizes");
        }
        else {
            throw new SRMException("for command "+command+" -sizes option is required");
        }
        
        String permanent;
        if(parser.isOptionSet(command,"permanent")) {
            permanent = parser.stringOptionValue(command,"permanent");
        }
        else {
            throw new SRMException("for command "+command+" -permanent option is required");
        }
        
        String protocols;
        if(parser.isOptionSet(command,"protocols")) {
            protocols = parser.stringOptionValue(command,"protocols");
        }
        else {
            throw new SRMException("for command "+command+" -protocols option is required");
        }
        
        String[] srcs_arr = strToArray(srcs);
        int length = srcs_arr.length;
        String[] dsts_arr = strToArray(dsts);
        if(length != dsts_arr.length) {
            throw new SRMException("for command "+command+
            " -dsts option gives incorrect number of elements :"+
            dsts);
        }
        // temporary use protocols_arr to store "sizes" and  "permanent" strings
        
        String[] protocols_arr = strToArray(sizes);
        
        long[] sizes_arr = new long[length];
        try {
            for(int i = 0; i<length;++i) {
                sizes_arr[i] = Long.parseLong(protocols_arr[i]);
                if(sizes_arr[i] < 0) {
                    throw new SRMException("for command "+command+
                    " -sizes option should contain a list of positive long integers :"+
                    sizes);
                }
            }
        }
        catch(NumberFormatException nfe) {
            throw new SRMException("for command "+command+
            " -sizes option should contain a list of positive long integers :"+
            sizes);
        }
        
        protocols_arr = strToArray(permanent);
        if(length != protocols_arr.length) {
            throw new SRMException("for command "+command+
            " -permanent option gives incorrect number of elements:"+
            permanent);
        }
        
        boolean[] permanent_arr = new boolean[length];
        for(int i = 0; i<length;++i) {
            permanent_arr[i] = protocols_arr[i].equalsIgnoreCase("true")||
            protocols_arr[i].equalsIgnoreCase("t");
        }
        
        protocols_arr = strToArray(protocols);
        if(length != protocols_arr.length) {
            throw new SRMException("for command "+command+
            " -protocols option gives incorrect number of elements:"+
            protocols);
        }
        
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.put(srcs_arr,dsts_arr,sizes_arr,permanent_arr,protocols_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    
    private static String executeMkPermanentCommand(String command,ArgParser parser) throws SRMException {
        String surls;
        if(parser.isOptionSet(command,"SURLS")) {
            surls = parser.stringOptionValue(command,"SURLS");
        }
        else {
            throw new SRMException("for command "+command+" -SURLS option is required");
        }
        System.out.println("surls ="+surls);
        
        String[] surls_arr = strToArray(surls);
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.mkPermanent(surls_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
        
    }
    private static String executePinCommand(String command,ArgParser parser) throws SRMException {
        String turls;
        if(parser.isOptionSet(command,"TURLS")) {
            turls = parser.stringOptionValue(command,"TURLS");
        }
        else {
            throw new SRMException("for command "+command+" -TURLS option is required");
        }
        System.out.println("turls ="+turls);
        
        String[] turls_arr = strToArray(turls);
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.pin(turls_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeUnpinCommand(String command,ArgParser parser) throws SRMException {
        int requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.intOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        System.out.println("requestID ="+requestID);
        
        String turls;
        if(parser.isOptionSet(command,"TURLS")) {
            turls = parser.stringOptionValue(command,"TURLS");
        }
        else {
            throw new SRMException("for command "+command+" -TURLS option is required");
        }
        System.out.println("turls ="+turls);
        
        String[] turls_arr = strToArray(turls);
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.unPin(turls_arr,requestID);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeGetRequestStatusCommand(String command,ArgParser parser) throws SRMException {
        int requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.intOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        System.out.println("requestID ="+requestID);
        
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.getRequestStatus(requestID);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeGetFileMetaDataCommand(String command,ArgParser parser) throws SRMException {
        String surls;
        if(parser.isOptionSet(command,"SURLS")) {
            surls = parser.stringOptionValue(command,"SURLS");
        }
        else {
            throw new SRMException("for command "+command+" -SURLS option is required");
        }
        System.out.println("surls ="+surls);
        
        String[] surls_arr = strToArray(surls);
        ISRM manager = connectToManager(parser);
        FileMetaData rs[] = manager.getFileMetaData(surls_arr);
        if(rs != null) {
            StringBuffer sb = new StringBuffer();
            
            for(int i = 0; i< rs.length; ++i) {
                sb.append("MetaData[").append(i).append("]=").append(rs[i]).append('\n');
            }
            return sb.toString();
        }
        else {
            return null;
        }
        
    }
    private static String executeGetProtocolsCommand(String command,ArgParser parser) throws SRMException {
        ISRM manager = connectToManager(parser);
        String protocols[] = manager.getProtocols();
        if(protocols != null) {
            StringBuffer sb = new StringBuffer();
            for(int i = 0;i<protocols.length;++i) {
                sb.append("protocol #").append(i).
                append(" : ").append(protocols[i]).append("\n");
            }
            return sb.toString();
        }
        else {
            return null;
        }
    }
    private static String executeGetEstGetTimeCommand(String command,ArgParser parser) throws SRMException {
        String surls;
        if(parser.isOptionSet(command,"SURLS")) {
            surls = parser.stringOptionValue(command,"SURLS");
        }
        else {
            throw new SRMException("for command "+command+" -SURLS option is required");
        }
        
        String protocols;
        if(parser.isOptionSet(command,"protocols")) {
            protocols = parser.stringOptionValue(command,"protocols");
        }
        else {
            throw new SRMException("for command "+command+" -protocols option is required");
        }
        
        
        String[] surls_arr = strToArray(surls);
        String[] protocols_arr = strToArray(protocols);
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.getEstGetTime(surls_arr,protocols_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeGetEstPutTimeCommand(String command,ArgParser parser) throws SRMException {
        String srcs;
        if(parser.isOptionSet(command,"srcs")) {
            srcs = parser.stringOptionValue(command,"srcs");
        }
        else {
            throw new SRMException("for command "+command+" -srcs option is required");
        }
        System.out.println("srcs ="+srcs);
        String dsts;
        if(parser.isOptionSet(command,"dsts")) {
            dsts = parser.stringOptionValue(command,"dsts");
        }
        else {
            throw new SRMException("for command "+command+" -dsts option is required");
        }
        String sizes;
        if(parser.isOptionSet(command,"sizes")) {
            sizes = parser.stringOptionValue(command,"sizes");
        }
        else {
            throw new SRMException("for command "+command+" -sizes option is required");
        }
        
        String permanent;
        if(parser.isOptionSet(command,"permanent")) {
            permanent = parser.stringOptionValue(command,"permanent");
        }
        else {
            throw new SRMException("for command "+command+" -permanent option is required");
        }
        
        String protocols;
        if(parser.isOptionSet(command,"protocols")) {
            protocols = parser.stringOptionValue(command,"protocols");
        }
        else {
            throw new SRMException("for command "+command+" -protocols option is required");
        }
        
        String[] srcs_arr = strToArray(srcs);
        int length = srcs_arr.length;
        String[] dsts_arr = strToArray(dsts);
        if(length != dsts_arr.length) {
            throw new SRMException("for command "+command+
            " -dsts option gives incorrect number of elements :"+
            dsts);
        }
        // temporary use protocols_arr to store "sizes" and  "permanent" strings
        
        String[] protocols_arr = strToArray(sizes);
        if(length != protocols_arr.length) {
            throw new SRMException("for command "+command+
            " -sizes option gives incorrect number of elements:"+
            sizes);
        }
        
        long[] sizes_arr = new long[length];
        try {
            for(int i = 0; i<length;++i) {
                sizes_arr[i] = Long.parseLong(protocols_arr[i]);
                if(sizes_arr[i] < 0) {
                    throw new SRMException("for command "+command+
                    " -sizes option should contain a list of positive long integers :"+
                    sizes);
                }
            }
        }
        catch(NumberFormatException nfe) {
            throw new SRMException("for command "+command+
            " -sizes option should contain a list of positive long integers :"+
            sizes);
        }
        
        protocols_arr = strToArray(permanent);
        if(length != protocols_arr.length) {
            throw new SRMException("for command "+command+
            " -permanent option gives incorrect number of elements:"+
            permanent);
        }
        
        boolean[] permanent_arr = new boolean[length];
        for(int i = 0; i<length;++i) {
            permanent_arr[i] = protocols_arr[i].equalsIgnoreCase("true")||
            protocols_arr[i].equalsIgnoreCase("t");
        }
        
        protocols_arr = strToArray(protocols);
        if(length != protocols_arr.length) {
            throw new SRMException("for command "+command+
            " -protocols option gives incorrect number of elements:"+
            protocols);
        }
        
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.getEstPutTime(srcs_arr,dsts_arr,sizes_arr,permanent_arr,protocols_arr);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeSetFileStatusCommand(String command,ArgParser parser) throws SRMException {
        int requestID;
        if(parser.isOptionSet(command,"requestID")) {
            requestID = parser.intOptionValue(command,"requestID");
        }
        else {
            throw new SRMException("for command "+command+" -requestID option is required");
        }
        
        int fileID;
        if(parser.isOptionSet(command,"fileID")) {
            fileID = parser.intOptionValue(command,"fileID");
        }
        else {
            throw new SRMException("for command "+command+" -fileID option is required");
        }
        
        String state;
        if(parser.isOptionSet(command,"state")) {
            state = parser.stringOptionValue(command,"state");
        }
        else {
            throw new SRMException("for command "+command+" -state option is required");
        }
        
        
        ISRM manager = connectToManager(parser);
        RequestStatus rs = manager.setFileStatus(requestID,fileID,state);
        if(rs != null) {
            return rs.toString();
        }
        else {
            return null;
        }
    }
    private static String executeAdvisoryDeleteCommand(String command,ArgParser parser) throws SRMException {
        
        return "not implemented";
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
            case PUT_COMMAND: {
                return executePutCommand(command,parser);
            }
            case MK_PERMANENT_COMMAND: {
                return executeMkPermanentCommand(command,parser);
            }
            case PIN_COMMAND: {
                return executePinCommand(command,parser);
            }
            case UNPIN_COMMAND: {
                return executeUnpinCommand(command,parser);
            }
            case GET_REQUEST_STATUS_COMMAND: {
                return executeGetRequestStatusCommand(command,parser);
            }
            case GET_FILE_METADATA_COMMAND: {
                return executeGetFileMetaDataCommand(command,parser);
            }
            case GET_PROTOCOLS_COMMAND: {
                return executeGetProtocolsCommand(command,parser);
            }
            case GET_EST_GET_TIME: {
                return executeGetEstGetTimeCommand(command,parser);
            }
            case GET_EST_PUT_TIME: {
                return executeGetEstPutTimeCommand(command,parser);
            }
            case SET_FILE_STATUS_COMMAND: {
                return executeSetFileStatusCommand(command,parser);
            }
            case ADVISORY_DELETE_COMMAND: {
                return executeAdvisoryDeleteCommand(command,parser);
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
    private static ISRM connectToManager(ArgParser parser) throws SRMException {
        String host;
        String glueprotocol="http";
        
        if( parser.isOptionSet(null,"srmhost")) {
            host = parser.stringOptionValue(null,"srmhost");
        }
        else {
            host = "localhost";
        }
        
        if( parser.isOptionSet(null,"gsissl")) {
            System.out.println("Installing tcp and ssl socket factories");
            ISocketFactory factory = new SslGsiSocketFactory();
            SocketFactories.addFactory("tcp", factory);
            SocketFactories.addFactory("ssl",factory);
            glueprotocol = " https";
            
        }
        else if(parser.isOptionSet(null,"gsiauth")) {
            System.out.println("Installing tcp and ssl socket factories");
            ISocketFactory factory = new GSSSocketFactory("ssl",new GssGsiCipherPlugin());
            SocketFactories.addFactory("tcp", factory);
            SocketFactories.addFactory("ssl",factory);
        }
        
        if( parser.isOptionSet(null,"glueprotocol")) {
            glueprotocol = parser.stringOptionValue(null,"glueprotocol");
        }
        
        short port = ClientV1.DEFAULTPORT;
        if(parser.isOptionSet(null,"srmport")) {
            port = (short)parser.intOptionValue(null,"srmport");
        }
        String srmpath =ClientV1.DEFAULTPATH;
        if(parser.isOptionSet(null,"srmpath")) {
            srmpath = parser.stringOptionValue(null,"srmpath");
        }
        
        ClientV1 client = new ClientV1(glueprotocol, host,port,srmpath);
        
        ISRM manager =  client.getManagerConnection();
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
        System.out.println("setting  electric.wsdl.targetNamespace to srm.1.0.ns");
        System.setProperty("electric.wsdl.targetNamespace","srm.1.0.ns");
        
        if(argv.length >2 && argv[1].equals("--help-command")) {
            command_usage = true;
        }
        
        for(int i=0;i<argv.length;++i) {
            if(argv[i].equalsIgnoreCase("-h") || argv[i].equalsIgnoreCase("-help") ||
            argv[i].equalsIgnoreCase("--h") || argv[i].equalsIgnoreCase("--help")) {
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
            System.out.println(parser.usage(argv[2]));
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
            System.exit(-1);
        }
        //System.out.println("parse succeded");
        String mapfile = null;
        if(parser.isOptionSet(null,"mapfile")) {
            mapfile = parser.stringOptionValue(null,"mapfile");
            try {
                electric.xml.io.Mappings.readMappings(mapfile);
                
            }
            catch(Exception e) {
                System.err.println("error reading mapping file "+mapfile);
                e.printStackTrace();
                System.err.println("srm_v1 --help for usage");
                System.exit(-1);
            }
        }
        
        
        try {
            System.out.println(executeCommand(parser));
        }
        catch(SRMException srme) {
            System.err.println("failed to execute command :"+srme.getMessage());
            System.err.println("srm -h for usage");
            return;
        }
        
    }
    public static String[] strToArray(String str) {
        if(str == null || str == "") {
            return null;
        }
        java.util.StringTokenizer st = new java.util.StringTokenizer(str,",");
        java.util.Vector v = new java.util.Vector();
        while(st.hasMoreTokens()) {
            v.add(st.nextToken());
        }
        String[] arr = new String[v.size()];
        v.toArray(arr);
        return arr;
    }
}
