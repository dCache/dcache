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

package gov.fnal.srm.util;

import org.globus.util.GlobusURL;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.dcache.srm.Logger;
import org.dcache.srm.util.SrmUrl;

/**
 *
 * @author  timur
 */

public class SRMDispatcher {
    public static final int ERROR_TYPE = -1;
    public static final int SRM_URL=0x1;
    public static final int HTTP_URL=0x2;
    public static final int SUPPORTED_PROTOCOL_URL=0x4;
    public static final int FILE_URL =0x8;
    public static final int EXISTS_FILE_URL=0x10;
    public static final int CAN_READ_FILE_URL=0x20;
    public static final int CAN_WRITE_FILE_URL=0x40;
    public static final int DIRECTORY_URL=0x80;
    public static final int UNKNOWN_URL=0x100;
    private SRMClient     srmclient;
    private Configuration configuration;
    private Logger        logger;

    private SRMDispatcher(Configuration configuration)
    {
        this.configuration = configuration;
        this.logger = configuration.getLogger();
    }

    private static class SRMCopyLogger implements Logger {
        private boolean debug;
        public SRMCopyLogger(boolean debug) {
            this.debug = debug;
        }

        @Override
        public void elog(String s) {
            System.err.println(s);
        }

        @Override
        public void elog(Throwable t) {
            t.printStackTrace(System.err);
        }

        @Override
        public void log(String s) {
            if (debug) {
                System.out.println(s);
            }
        }
    }

    public static final void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            conf.parseArguments(args);
            Logger logger=new SRMCopyLogger(conf.isDebug());
            conf.setLogger(logger);
            logger.log(conf.toString());
            String globus_tcp_port_range=conf.getGlobus_tcp_port_range();
            if(globus_tcp_port_range != null) {
                System.setProperty("org.globus.tcp.port.range",globus_tcp_port_range);
            }
            String trusted_ca_certs = conf.getX509_user_trusted_certificates();
            if ( trusted_ca_certs != null ) {
                System.setProperty("X509_CERT_DIR",trusted_ca_certs);
            }
            if(conf.getSave_config_file() != null) {
                logger.log(" saving configuration in "+
                        conf.getSave_config_file());
                conf.write(conf.getSave_config_file());
                System.exit(0);
            }
            if(conf.isHelp()) {
                logger.elog(conf.usage());
                System.exit(1);
            }
            if (conf.isGetFileMetaData()) {
                if(conf.getGetFileMetaDataSurls() == null ||
                        conf.getGetFileMetaDataSurls().length == 0 ) {
                    System.err.println(
                    "cannot get metadata - surls not specified");
                    System.exit(1);
                }
            }
            else if (conf.isGetPermission()){
                if(conf.getGetPermissionSurls() == null ||
                        conf.getGetPermissionSurls().length == 0 ) {
                    System.err.println("cannot get permission - surls not specified");
                    System.exit(1);
                }
            }
            else if (conf.isCheckPermission()){
                if(conf.getCheckPermissionSurls() == null ||
                        conf.getCheckPermissionSurls().length == 0 ) {
                    System.err.println("cannot check permission - surls not specified");
                    System.exit(1);
                }
            }
            else if (conf.isExtendFileLifetime()){
                if(conf.getExtendFileLifetimeSurls() == null ||
                        conf.getExtendFileLifetimeSurls().length == 0 ) {
                    System.err.println("cannot extend file lifetime - surls not specified");
                    System.exit(1);
                }
            }
            else if (conf.isSetPermission()){
                if(conf.getSetPermissionSurl() == null) {
                    System.err.println("cannot set permission - surls not specified");
                    System.exit(1);
                }
                if (conf.getSetPermissionType()==null) {
                    System.err.println("permission type is not specified");
                    System.exit(1);
                }
                String types[] = {"ADD","REMOVE","CHANGE"};
                boolean ok=false;
                for (String p : types) {
                    if (conf.getSetPermissionType().equalsIgnoreCase(p)) {
                        ok = true;
                        break;
                    }
                }
                if ( ok == false ) {
                    StringBuilder sb=new StringBuilder();
                    sb.append("Incorrect permission type specified ")
                            .append(conf.getSetPermissionType()).append("\n");
                    sb.append("supported permission types :\n");
                    sb.append("\t");
                    for (String p : types) {
                        sb.append(p).append(" ");
                    }
                    sb.append("\n");
                    System.err.println(sb.toString());
                    System.exit(1);

                }
                String modes[] = {"NONE","X","W","WR","R","RX","RW","RWX"};
                ok=false;
                if ( conf.getSetOwnerPermissionMode() != null ) {
                    for (String m : modes) {
                        if (conf.getSetOwnerPermissionMode()
                                .equalsIgnoreCase(m)) {
                            ok = true;
                            break;

                        }
                    }
                    if ( ok == false ) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("Incorrect owner permission mode specified ")
                                .append(conf.getSetOwnerPermissionMode())
                                .append("\n");
                        sb.append("supported owner permission modes :\n");
                        sb.append("\t");
                        for (String m : modes) {
                            sb.append(m).append(" ");
                        }
                        sb.append("\n");
                        System.err.println(sb.toString());
                        System.exit(1);
                    }
                }
                ok=false;
                if ( conf.getSetGroupPermissionMode() != null ) {
                    for (String m : modes) {
                        if (conf.getSetGroupPermissionMode()
                                .equalsIgnoreCase(m)) {
                            ok = true;
                            break;

                        }
                    }
                    if ( ok == false ) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("Incorrect group permission mode specified ")
                                .append(conf.getSetGroupPermissionMode())
                                .append("\n");
                        sb.append("supported group permission modes :\n");
                        sb.append("\t");
                        for (String m : modes) {
                            sb.append(m).append(" ");
                        }
                        sb.append("\n");
                        System.err.println(sb.toString());
                        System.exit(1);
                    }
                }
                ok=false;
                if ( conf.getSetOtherPermissionMode() != null ) {
                    for (String m : modes) {
                        if (conf.getSetOtherPermissionMode()
                                .equalsIgnoreCase(m)) {
                            ok = true;
                            break;

                        }
                    }
                    if ( ok == false ) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("Incorrect other permission mode specified ")
                                .append(conf.getSetOtherPermissionMode())
                                .append("\n");
                        sb.append("supported other permission modes :\n");
                        sb.append("\t");
                        for (String m : modes) {
                            sb.append(m).append(" ");
                        }
                        sb.append("\n");
                        System.err.println(sb.toString());
                        System.exit(1);
                    }
                }
            }
            else if (conf.isAdvisoryDelete()) {
                if (conf.getAdvisoryDeleteSurls() == null ||
                        conf.getAdvisoryDeleteSurls().length == 0 ) {
                    System.err.println(
                    "cannot perform advisory delete - surls not specified");
                    System.exit(1);
                }
            }
            else if (conf.isGetRequestStatus()) {
                if (conf.getGetRequestStatusSurl() == null ) {
                    System.err.println("surl, to be used for location of srm, is not specified");
                    System.exit(1);
                }
            }
            else if (conf.isGetRequestSummary()) {
                if (conf.getGetRequestStatusSurl() == null ) {
                    System.err.println("surl, to be used for location of srm, is not specified");
                    System.exit(1);
                }
            }
            else if (conf.isGetRequestTokens()) {
                if (conf.getGetRequestStatusSurl() == null ) {
                    System.err.println("surl, to be used for location of srm, is not specified");
                    System.exit(1);
                }
            }
            else if (conf.isCopy()||conf.isMove()) {
                if((conf.getFrom() == null || conf.getTo() == null) &&
                        conf.getCopyjobfile() == null ) {
                    logger.elog("source(s) and/or destination are not specified in "+
                    "either argument or file");
                    System.exit(1);
                }
            }
            else if (conf.isReserveSpace()) {
                if(conf.getGuaranteedReserveSpaceSize()<=0) {
                    logger.elog("guaranteed size of space reservation must be greater than 0");
                    logger.elog(conf.usage());

                    System.exit(1);
                }
            }
            else if (conf.isReleaseSpace()) {
                if(conf.getSpaceToken()==null || conf.getSpaceToken().trim().equals("")) {
                    logger.elog("space token has to be specified to release explicit space reservation");
                    System.exit(1);
                }
            }
            else if (conf.isGetSpaceMetaData()) {
                if(conf.getSrmUrl()==null ) {
                    logger.elog("srm url identifying srm system must be specified");
                    System.exit(1);
                }
                if(conf.getSpaceTokensList()==null ||conf.getSpaceTokensList().length <1 ) {
                    logger.elog("at least one space token must be specified");
                    System.exit(1);
                }
            }
            else if (conf.isGetSpaceTokens()) {
                if(conf.getSrmUrl()==null ) {
                    logger.elog("srm url identifying srm system must be specified");
                    System.exit(1);
                }
            }
        }
        catch(Exception e) {
            if(conf.isDebug()) {
                throw e;
            }
            else {
                System.err.println("srm client error: \n" + e.toString());
                System.exit(1);
            }
        }

        int repeatCount=conf.getRepeatCount();
        long endTime = System.currentTimeMillis();
        long startTime = endTime;
        Exception exception = null;
        String clientName = null;
        if(conf.isPrintPerfomance()) {
            System.out.format("%25s, %19s, %19s, %5s, %s, %s\n",
                    "<String(performace info)>","<StartMillis>",
                    "<EndMillis>","<RunNumber>","<Status(success|failure)",
            "<performanceTestName>");
        }
        for(int i=0; i<repeatCount; i++) {
            exception = null;
            try {

                SRMDispatcher dispatcher = new SRMDispatcher(conf);
                dispatcher.work();
                clientName = dispatcher.getClientName();
            }
            catch(Exception e) {
                exception = e;
                if(conf.isDebug()) {
                    e.printStackTrace();
                }
                else {
                    System.err.println("srm client error: \n" + e.toString());
                }
            }
            finally {
                if(conf.isPrintPerfomance()) {
                    String perfomanceTestName = conf.getPerformanceTestName();
                    if(perfomanceTestName  == null) {
                        perfomanceTestName = clientName;
                    }
                    String success = exception == null? "success":"failure";
                    endTime = System.currentTimeMillis();
                    System.out.format("%25s, %19d, %19d, %5d, %s, %s\n",
                            "performace info", startTime,endTime,i,success,perfomanceTestName );
                    startTime = endTime;
                }
            }
        }

        if(exception != null) {
            System.exit(1);
        }
    }

    private void work() throws Exception {
        if (configuration.isGetFileMetaData()) {
            String[] surl_strings = configuration.getGetFileMetaDataSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls     = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMGetFileMetaDataClientV1(configuration, surls, surl_strings);
        }
        else if (configuration.isGetPermission()) {
            String[] surl_strings = configuration.getGetPermissionSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls     = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMGetPermissionClientV2(configuration, surls,surl_strings);
        }
        else if (configuration.isCheckPermission()) {
            String[] surl_strings = configuration.getCheckPermissionSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls     = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMCheckPermissionClientV2(configuration, surls,surl_strings);
        }
        else if (configuration.isExtendFileLifetime()) {
            String[] surl_strings = configuration.getExtendFileLifetimeSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls     = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMExtendFileLifeTimeClientV2(configuration, surls,surl_strings);
        }
        else if (configuration.isSetPermission()) {
            String surl_string = configuration.getSetPermissionSurl();
            GlobusURL surl     = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient = new SRMSetPermissionClientV2(configuration, surl,surl_string);
        }
        else if (configuration.isls()) {
            String[] surl_strings  = configuration.getLsURLs();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient              = new SRMLsClientV2(configuration, surls, surl_strings);
        }
        else if (configuration.isReserveSpace()) {
            String[] surl_strings  = configuration.getReserveSpaceURLs();
            if (  surl_strings == null ) {
                throw new IllegalArgumentException("Must specify SRM URL" ) ;
            }
            int number_of_surls    = surl_strings.length;
            if ( number_of_surls > 1  ) {
                throw new IllegalArgumentException("Only one SRM SURL is  supported " ) ;
            }
            else if ( number_of_surls == 0  ) {
                throw new IllegalArgumentException("No URL specified ");
            }
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient              = new SRMReserveSpaceClientV2(configuration, surls[0]);
        }
        else if (configuration.isReleaseSpace()) {
            String[] surl_strings  = configuration.getReserveSpaceURLs();
            if (  surl_strings == null ) {
                throw new IllegalArgumentException("Must specify SRM URL" ) ;
            }
            int number_of_surls    = surl_strings.length;
            if ( number_of_surls > 1  ) {
                throw new IllegalArgumentException("Only one SRM SURL is  supported " ) ;
            }
            else if ( number_of_surls == 0  ) {
                throw new IllegalArgumentException("No URL specified ");
            }
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient              = new SRMReleaseSpaceClientV2(configuration, surls[0]);
        }
        else if (configuration.isGetSpaceMetaData()) {
            String surl_string  = configuration.getSrmUrl();
            if (  surl_string == null ) {
                throw new IllegalArgumentException("Must specify SRM URL" ) ;
            }
            GlobusURL surl      = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient              = new SRMGetSpaceMetaDataClientV2(configuration, surl);
        }
        else if (configuration.isGetSpaceTokens()) {
            String surl_string  = configuration.getSrmUrl();
            if (  surl_string == null ) {
                throw new IllegalArgumentException("Must specify SRM URL" ) ;
            }
            GlobusURL surl      = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient              = new SRMGetSpaceTokensClientV2(configuration, surl);
        }
        else if (configuration.isStage()) {
            String[] surl_strings  = configuration.getLsURLs();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient              = new SRMStageClientV1(configuration, surls);
        }
        else if (configuration.isRmdir()) {
            String[] surl_strings  = configuration.getRmURLs();
            int number_of_surls    = surl_strings.length;
            if ( number_of_surls > 1 ) {
                throw new IllegalArgumentException("Only single directory tree removal is suported " ) ;
            }
            else if ( number_of_surls == 0  ) {
                throw new IllegalArgumentException("No URL specified");
            }
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMRmdirClientV2(configuration, surls[0], surl_strings[0]);
        }
        else if (configuration.isMkdir()) {
            String[] surl_strings  = configuration.getMkDirURLs();
            int number_of_surls    = surl_strings.length;
            if ( number_of_surls > 1  ) {
                throw new IllegalArgumentException("Only one directory at a time supported " ) ;
            }
            else if ( number_of_surls == 0  ) {
                throw new IllegalArgumentException("No URL specified ");
            }
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMMkDirClientV2(configuration, surls[0], surl_strings[0]);
        }
        else if (configuration.isRm()) {
            String[] surl_strings  = configuration.getRmURLs();
            int number_of_surls    = surl_strings.length;
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMRmClientV2(configuration, surls, surl_strings);
        }
        else if(configuration.isAdvisoryDelete()) {
            String[] surl_strings = configuration.getAdvisoryDeleteSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls     = new GlobusURL[number_of_surls];
            for (int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient = new SRMAdvisoryDeleteClientV1(configuration, surls, surl_strings);
        }
        else if(configuration.isGetRequestStatus()) {
            String surl_string = configuration.getGetRequestStatusSurl();
            int requestId      = configuration.getGetRequestStatusId();
            GlobusURL surl     = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());

            srmclient          = new SRMGetRequestStatusClientV1(configuration, surl,requestId);
        }
        else if(configuration.isGetRequestSummary()) {
            String surl_string = configuration.getGetRequestStatusSurl();
            GlobusURL surl     = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient          = new SRMGetRequestSummaryClientV2(configuration, surl);
        }
        else if(configuration.isGetRequestTokens()) {
            String surl_string = configuration.getGetRequestStatusSurl();
            GlobusURL surl     = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient          = new SRMGetRequestTokensClientV2(configuration, surl);
        }
        else if (configuration.isMove()) {
            String from;
            String to;
            String[] surl_strings = new String[2];
            if(configuration.getCopyjobfile() != null) {
                CopyJobFileReader reader = new CopyJobFileReader(configuration.getCopyjobfile());
                if (reader.getSources().length>1||reader.getDestinations().length>1) {
                    throw new IllegalArgumentException("only signle source and single destination supported");
                }
                from = reader.getSources()[0];
                to   = reader.getDestinations()[0];
            }
            else {
                if (configuration.getFrom().length>1) {
                    throw new IllegalArgumentException("only signle source and single destination supported");
                }

                from           = configuration.getFrom()[0];
                to             = configuration.getTo();
            }
            GlobusURL from_url = new SrmUrl(from,
                    configuration.getDefaultSrmPortNumber());
            GlobusURL to_url   = new SrmUrl(to,
                    configuration.getDefaultSrmPortNumber());
            GlobusURL[] surls  = new GlobusURL[2];
            surls[0] = from_url;
            surls[1] = to_url;
            surl_strings[0]=from;
            surl_strings[1]=to;
            int fromType = getUrlType(from_url);
            int toType   = getUrlType(to_url);
            if ( fromType != toType ) {
                throw new IllegalArgumentException("source and destination have to have same URL type");
            }
            if ( fromType != SRM_URL ) {
                throw new IllegalArgumentException("source and destination have to be SRM URLs");
            }
            checkURLSUniformity(fromType,surls,true);
            srmclient = new SRMMvClientV2(configuration, surls, surl_strings);
        }
        else if(configuration.isCopy()) {
            GlobusURL[] from_urls;
            GlobusURL[] to_urls;
            if(configuration.getCopyjobfile() != null) {
                CopyJobFileReader reader = new CopyJobFileReader(configuration.getCopyjobfile());
                String[] from            = reader.getSources();
                String[] to              = reader.getDestinations();
                int number_of_sources    = from.length;
                from_urls                = new GlobusURL[number_of_sources];
                to_urls                  = new GlobusURL[number_of_sources];
                for(int i=0;i<number_of_sources;++i) {
                    from_urls[i] = new SrmUrl(from[i],
                            configuration.getDefaultSrmPortNumber());
                    to_urls[i]   = new SrmUrl(to[i],
                            configuration.getDefaultSrmPortNumber());
                }
            }
            else {
                String[] from         = configuration.getFrom();
                int number_of_sources = from.length;
                String to             = configuration.getTo();
                from_urls             = new GlobusURL[number_of_sources];
                for (int i=0;i<number_of_sources;++i) {
                    from_urls[i] = new SrmUrl(from[i],
                            configuration.getDefaultSrmPortNumber());
                }
                to_urls = new GlobusURL[number_of_sources];
                if (number_of_sources >1) {
                    for (int i=0;i<number_of_sources;++i) {
                        String file   = from_urls[i].getPath();
                        int lastSlash = file.lastIndexOf('/');
                        if(lastSlash != -1) {
                            file = file.substring(lastSlash);
                        }
                        to_urls[i] = new SrmUrl(to+"/"+file,
                                configuration.getDefaultSrmPortNumber());
                    }
                }
                else {
                    to_urls[0] =  new  SrmUrl(to,
                            configuration.getDefaultSrmPortNumber());
                }
            }
            int fromType = getUrlType(from_urls[0]);
            checkURLSUniformity(fromType,from_urls,true);
            int toType = getUrlType(to_urls[0]);
            checkURLSUniformity(toType,to_urls,false);

            if (fromType == SRM_URL ) {
                if ((toType & FILE_URL) == FILE_URL) {
                    dsay("starting SRMGetClient");
                    if(configuration.getSrmProtocolVersion() == 1) {
                        srmclient = new SRMGetClientV1(configuration,from_urls,to_urls);
                    } else if(configuration.getSrmProtocolVersion() == 2) {
                        srmclient = new SRMGetClientV2(configuration,from_urls,to_urls);
                    }
                }
                else {
                    if (toType == SRM_URL) {
                        // both source(s) and destination(s) are srm urls
                        // we can either push or pull
                        if (configuration.getSrmProtocolVersion() == 1) {
                            srmclient = new SRMCopyClientV1(configuration,from_urls,to_urls);
                        }
                        else  if ( configuration.getSrmProtocolVersion() == 2 )  {
                            srmclient = new SRMCopyClientV2(configuration,from_urls,to_urls);
                        }
                    }
                    else {
                        //
                        // Dmitry's kludge
                        //
                        if (!configuration.isPushmode()) {
                            configuration.setPushmode(true);
                        }
                        if (configuration.getSrmProtocolVersion() == 1) {
                            srmclient = new SRMCopyClientV1(configuration,from_urls,to_urls);
                        }
                        else  if ( configuration.getSrmProtocolVersion() == 2 )  {
                            srmclient = new SRMCopyClientV2(configuration,from_urls,to_urls);
                        }
                    }
                }
            }
            else if(toType == SRM_URL) {
                if ((fromType & FILE_URL) == FILE_URL) {
                    if(configuration.getSrmProtocolVersion() == 1) {
                        srmclient = new SRMPutClientV1(configuration,from_urls,to_urls);
                    } else if(configuration.getSrmProtocolVersion() == 2) {
                        srmclient = new SRMPutClientV2(configuration,from_urls,to_urls);
                    }
                }
                else {
                    //
                    // Dmitry's kludge
                    //
                    if (configuration.isPushmode()) {
                        configuration.setPushmode(false);
                    }
                    if(configuration.getSrmProtocolVersion() == 1) {
                        srmclient = new SRMCopyClientV1(configuration,from_urls,to_urls);
                    }
                    else  if ( configuration.getSrmProtocolVersion() == 2 )  {
                        srmclient = new SRMCopyClientV2(configuration,from_urls,to_urls);
                    }
                }
            }
            else if(((fromType & FILE_URL) == FILE_URL ||
                    (fromType & SUPPORTED_PROTOCOL_URL)== SUPPORTED_PROTOCOL_URL) &&
                    ((toType & FILE_URL) == FILE_URL ||
                            (toType & SUPPORTED_PROTOCOL_URL)== SUPPORTED_PROTOCOL_URL))
            {
                srmclient = new SRMSimpleCopyClient(configuration,from_urls,to_urls);
            }
            else {

                esay("neither source nor destination are SRM URLs :"+
                        from_urls[0].getURL()+" "+from_urls[0].getURL());
                throw new IllegalArgumentException(
                        "neither source nor destination are SRM URLs :"+
                        from_urls[0].getURL()+" "+from_urls[0].getURL());
            }
        }
        else if (configuration.isBringOnline()) {
            String[] surl_strings  = configuration.getBringOnlineSurls();
            int number_of_surls   = surl_strings.length;
            GlobusURL[] surls      = new GlobusURL[number_of_surls];
            for(int i=0;i<number_of_surls;++i) {
                surls[i] = new SrmUrl(surl_strings[i],
                        configuration.getDefaultSrmPortNumber());
            }
            checkURLSUniformity(SRM_URL, surls, false);
            srmclient              = new SRMBringOnlineClientV2(configuration, surls);
        }
        else if(configuration.isPing()) {
            String surl_string = configuration.getSrmUrl();
            GlobusURL surl = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            if(configuration.getSrmProtocolVersion() == 1) {
                srmclient  = new SRMPingClientV1(configuration,surl);
            }
            if(configuration.getSrmProtocolVersion() == 2) {
                srmclient  = new SRMPingClientV2(configuration,surl);
            }
        }
        else if (configuration.isAbortRequest()) {
            String surl_string  = configuration.getSrmUrl();
            if (  surl_string == null ) {
                throw new IllegalArgumentException("Must specify SRM URL" ) ;
            }
            GlobusURL surl      = new SrmUrl(surl_string,
                    configuration.getDefaultSrmPortNumber());
            srmclient           = new SRMAbortRequestClientV2(configuration, surl);
        }
        else if (configuration.isAbortFiles()) {
            srmclient           = new SRMAbortFilesClientV2(configuration);
        }
        else if (configuration.isReleaseFiles()) {
            srmclient           = new SRMReleaseFilesClientV2(configuration);
        }
        else {
            System.err.println(" unknown action requested");
            System.exit(1);
        }
        srmclient.connect();
        srmclient.start();
    }

    public void  checkURLSUniformity(int type,GlobusURL urls[],boolean areSources) throws Exception {
        int number_of_sources = urls.length;
        if (number_of_sources==0) {
            throw new IllegalArgumentException("No URL(s) specified ");
        }
        String host = urls[0].getHost();
        int port = urls[0].getPort();
        if(type == SRM_URL  || ((type & SUPPORTED_PROTOCOL_URL) == SUPPORTED_PROTOCOL_URL)) {
            if( host == null || host.equals("") ||
                    port < 0) {
                String error = "illegal source url for multiple sources mode"+
                urls[0].getURL();
                esay(error);
                throw new IllegalArgumentException(error );
            }

            for (GlobusURL url : urls) {
                if (type != getUrlType(url)) {
                    String error = "if specifying multiple sources/destinations," +
                            " sources/destinations must be of the same type, incorrect url: " +
                            url;
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                if (!host.equals(url.getHost())) {
                    String error = "if specifying multiple  sources, " +
                            "all sources must have same host" +
                            url.getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                if (port != url.getPort()) {
                    String error =
                            "if specifying multiple  sources, " +
                                    "all sources must have same port" +
                                    url;
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
            }
        }
        else if((type & FILE_URL) == FILE_URL) {
            for(int i=1; i<number_of_sources; ++i) {
                int thisTypeI =
                    (i==0?type:getUrlType(urls[i]));
                if((thisTypeI & FILE_URL) != FILE_URL) {
                    String error =
                        "If specifying multiple sources, sources must be " +
                        "of the same type.  Incorrect source: "+
                        urls[i] ;
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                if((thisTypeI & DIRECTORY_URL) == DIRECTORY_URL) {
                    String error = "source/destination file is directory"+
                    urls[i].getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                if(areSources && ((thisTypeI & EXISTS_FILE_URL) == 0)) {
                    String error = "source file does not exist"+urls[i].getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
                if(areSources &&((thisTypeI & CAN_READ_FILE_URL) == 0)) {
                    String error = "source file is not readable"+urls[i].getURL();
                    esay(error);
                    throw new IllegalArgumentException(error);
                }
            }
        }
        else {
            String error = "Unknown type of source(s) or destination(s)";
            esay(error);
            throw new IllegalArgumentException(error);
        }
        if(configuration.getCopyjobfile() == null) {
            for(int i = 0; i<number_of_sources; ++i) {
                for(int j = 0;j<number_of_sources; ++j) {
                    if(i != j && (urls[i].getPath().equals(urls[j].getPath()))) {
                        String error = "list of sources contains the same url twice "+
                        "url#"+i+" is "+urls[i].getURL() +
                        " and url#"+j+" is "+urls[j].getURL();
                        esay(error);
                        throw new IllegalArgumentException(error);
                    }
                }
            }
        }
    }

    public final void say(String msg) {
        logger.log(new Date().toString() +": "+msg);
    }

    //say if debug
    public  final void dsay(String msg) {
        if(configuration.isDebug()) {
            logger.log(new Date().toString() +": "+msg);
        }
    }

    //error say
    public final void esay(String err) {
        logger.elog(new Date().toString() +": "+err);
    }

    //esay if debug
    public  final void edsay(String err) {
        if(configuration.isDebug()) {
            logger.elog(new Date().toString() +": "+err);
        }
    }


    public static int getUrlType(GlobusURL url) throws IOException {
        String prot = url.getProtocol();
        if(prot != null ) {
            if(prot.equals("srm")) {
                return SRM_URL;
            } else if(prot.equals("http")   ||
                    prot.equals("ftp")    ||
                    prot.equals("gsiftp") ||
                    prot.equals("gridftp")||
                    prot.equals("https")  ||
                    prot.equals("ldap")   ||
                    prot.equals("ldaps")  ||
                    prot.equals("dcap")   ||
                    prot.equals("rfio")) {
                return SUPPORTED_PROTOCOL_URL;
            }
            else if(!prot.equals("file")) {
                return UNKNOWN_URL;
            }
        }

        File f = new File(url.getPath());
        f = f.getCanonicalFile();
        int rc = FILE_URL;
        if(f.exists()) {
            rc |= EXISTS_FILE_URL;
        }
        if(f.canRead()) {
            rc |= CAN_READ_FILE_URL;
        }
        if(f.canWrite()) {
            rc |= CAN_WRITE_FILE_URL;
        }
        if(f.isDirectory()) {
            rc |= DIRECTORY_URL;
        }
        return rc;
    }

    public String getClientName() {
        if(srmclient == null) {
            return "unknown";
        }
        return srmclient.getClass().getSimpleName();
    }
}


