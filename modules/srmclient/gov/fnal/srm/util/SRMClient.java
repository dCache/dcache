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

import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.ISRM;
import org.dcache.srm.client.SRMClientV1;
import java.io.IOException;
import org.globus.util.GlobusURL;

/**
 *
 * @author  timur
 */
public abstract class SRMClient {
    private String glueprotocol;
    private String gluepath;
    private boolean gsissl;
    protected boolean debug;
    protected ISRM srm;
    private GlobusURL srm_url;
    protected String urlcopy;
    protected Configuration configuration;
    protected org.dcache.srm.Logger logger;
    protected boolean doDelegation =false;
    protected boolean fullDelegation =false;
    protected String gss_expected_name ="host";
    protected long retrytimeout=1000;
    protected int retries = 10;
    protected Report report;


    public SRMClient(Configuration configuration) {
        this.configuration = configuration;
        logger = configuration.getLogger();
        this.glueprotocol = configuration.getWebservice_protocol();
        this.gluepath = configuration.getWebservice_path();
        this.gsissl = configuration.isGsissl();
        this.debug=configuration.isDebug();
        this.urlcopy=configuration.getUrlcopy();
        this.doDelegation = configuration.isDelegate();
        this.fullDelegation = configuration.isFull_delegation();
        this.gss_expected_name = configuration.getGss_expected_name();

        dsay("In SRMClient ExpectedName: "+gss_expected_name);
        dsay("SRMClient("+glueprotocol+","+gluepath+","+gsissl+")");
    }

    public void setUrlcopy(String urlcopy) {
        this.urlcopy = urlcopy;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public static RequestFileStatus getFileRequest(RequestStatus rs,
                                                   Integer nextID) {

        RequestFileStatus[] frs = rs.fileStatuses;
        if(frs == null ) {
            return null;
        }

        for(int i= 0; i<frs.length;++i) {
            if(frs[i].fileId ==  nextID.intValue()) {
                return frs[i];
            }
        }
        return null;
    }


    public final void say(String msg) {
        logger.log(new java.util.Date().toString() +": "+msg);
    }

    //say if debug
    public  final void dsay(String msg) {
        if(debug) {
            logger.log(new java.util.Date().toString() +": "+msg);
        }
    }

    //error say
    public final void esay(String err) {
        logger.elog(new java.util.Date().toString() +": "+err);
    }

    //esay if debug
    public  final void edsay(String err) {
        if(debug) {
            logger.elog(new java.util.Date().toString() +": "+err);
        }
    }

    public abstract void connect() throws Exception;

    public abstract void start() throws Exception;

    protected void connect(GlobusURL srmUrl) throws Exception {
        try {

            SRMClientV1 client;
            client = new SRMClientV1(srmUrl, getGssCredential(),configuration.getRetry_timeout(),configuration.getRetry_num(),doDelegation, fullDelegation,gss_expected_name,configuration.getWebservice_path());
            dsay("connected to server, obtaining proxy");

            //srm =  client.getManagerConnection ();
            srm = client;
            srm_url = srmUrl;
            dsay("got proxy of type "+srm.getClass());

        } catch (Exception srme) {
            throw new IOException(srme.toString());
        }
        if(srm == null) {
            throw new IOException("can not get manager connection");
        }

    }

    public org.ietf.jgss.GSSCredential  getGssCredential()
    throws Exception {
        if(configuration.isUseproxy()) {
            return org.dcache.srm.security.SslGsiSocketFactory.
            createUserCredential(configuration.getX509_user_proxy(),
                    null,
                    null);
        } else {
            return org.dcache.srm.security.SslGsiSocketFactory.
            createUserCredential(
                    null,
                    configuration.getX509_user_cert(),
                    configuration.getX509_user_key());
        }
    }

    public  void done(RequestStatus rs,ISRM srm) {
        if(rs.fileStatuses != null) {
            for(int i = 0; i< rs.fileStatuses.length;++i) {
                RequestFileStatus rfs = rs.fileStatuses[i];
                if(!rfs.state.equals("Done") &&
                        !rfs.state.equals("Failed")) {
                    say("rfs.state is " + rfs.state +
                            " calling setFileStatus(" + rs.requestId + "," +
                            rfs.fileId + ",\"Done\")");
                    srm.setFileStatus(rs.requestId,rfs.fileId,"Done");
                }
            }
        }
    }

    private void setReportSuccessStatusBySource(GlobusURL url){
        if(report == null) return;
        report.setStatusBySourceUrl(url, Report.OK_RC, null);

    }

    private void setReportSuccessStatusByDest(GlobusURL url){
        if(report == null) return;
        report.setStatusByDestinationUrl(url, Report.OK_RC, null);

    }
    private void setReportSuccessStatusBySrcAndDest(GlobusURL srcurl,
                                                    GlobusURL dsturl){
        if(srcurl == null ) {
            setReportSuccessStatusByDest(dsturl);
            return;
        }
        if(dsturl == null ) {
            setReportSuccessStatusBySource(srcurl);
            return;
        }

        if(report == null) return;



        report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.OK_RC, null);
        return;
    }

    private void setReportFailedStatusBySource(GlobusURL url, String error){
        if(report == null) return;
        if(error == null) {
            report.setStatusBySourceUrl(url, Report.ERROR_RC, "unknonw error");
            return;
        }
        error.replace('\n', ' ');
        if(error.toLowerCase().indexOf("file exists") != -1 ) {
            report.setStatusBySourceUrl(url, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().indexOf("permission") != -1 ) {
            report.setStatusBySourceUrl(url, Report.PERMISSION_RC, error);
            return;
        }
        report.setStatusBySourceUrl(url, Report.ERROR_RC, error);
        return;
    }

    private void setReportFailedStatusByDest(GlobusURL url, String error){
        if(report == null) return;
        if(error == null) {
            report.setStatusByDestinationUrl(url, Report.ERROR_RC, "unknonw error");
            return;
        }
        error.replace('\n', ' ');
        if(error.toLowerCase().indexOf("file exists") != -1 ) {
            report.setStatusByDestinationUrl(url, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().indexOf("permission") != -1 ) {
            report.setStatusByDestinationUrl(url, Report.PERMISSION_RC, error);
            return;
        }

        report.setStatusByDestinationUrl(url, Report.ERROR_RC, error);
        return;
    }

    private void setReportFailedStatusBySrcAndDest(GlobusURL srcurl, GlobusURL dsturl, String error){
        if(srcurl == null ) {
            setReportFailedStatusByDest(dsturl,error);
            return;
        }
        if(dsturl == null ) {
            setReportFailedStatusBySource(srcurl,error);
            return;
        }

        if(report == null) return;


        if(error == null) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.ERROR_RC, "unknonw error");
            return;
        }
        error.replace('\n', ' ');
        if(error.toLowerCase().indexOf("file exists") != -1 ) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().indexOf("permission") != -1 ) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.PERMISSION_RC, error);
            return;
        }

        report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.ERROR_RC, error);
        return;
    }

    protected void setReportFailed(GlobusURL srcurl,GlobusURL dsturl,String error ) {
        try {
            setReportFailedStatusBySrcAndDest(srcurl,dsturl, error);
        } catch(Exception e) {
            //e.printStackTrace();
            try {
                setReportFailedStatusByDest(dsturl, error);
            } catch(Exception e1){
                //e1.printStackTrace();
                setReportFailedStatusBySource(srcurl,error);
            }
        }
    }

    protected void setReportSucceeded(GlobusURL srcurl,
                                      GlobusURL dsturl) {
        try {
            setReportSuccessStatusBySrcAndDest(srcurl,dsturl);
        } catch(Exception e) {
            try {
                setReportSuccessStatusByDest(dsturl);
            } catch(Exception e1){
                setReportSuccessStatusBySource(srcurl);
            }
        }
    }


}

