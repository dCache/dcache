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

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;

import java.net.URI;
import java.util.Date;

import org.dcache.srm.Logger;
import org.dcache.srm.client.Transport;
import org.dcache.srm.client.TransportUtil;

/**
 *
 * @author  timur
 */
public abstract class SRMClient {

    protected boolean debug;
    protected String urlcopy;
    protected Configuration configuration;
    protected Logger logger;
    protected boolean doDelegation;
    protected boolean fullDelegation;
    protected String gss_expected_name;
    protected Report report;


    public SRMClient(Configuration configuration) {
        this.configuration = configuration;
        logger = configuration.getLogger();
        this.debug=configuration.isDebug();
        this.urlcopy=configuration.getUrlcopy();
        this.doDelegation = configuration.isDelegate();
        this.fullDelegation = configuration.isFull_delegation();
        this.gss_expected_name = configuration.getGss_expected_name();

        Transport transport = configuration.getTransport();
        dsay("In SRMClient ExpectedName: "+gss_expected_name);
        dsay("SRMClient("+TransportUtil.uriSchemaFor(transport)+","+transport.toString()+")");
    }

    public void setUrlcopy(String urlcopy) {
        this.urlcopy = urlcopy;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public final void say(String msg) {
        logger.log(new Date().toString() +": "+msg);
    }

    //say if debug
    public  final void dsay(String msg) {
        if(debug) {
            logger.log(new Date().toString() +": "+msg);
        }
    }

    //error say
    public final void esay(String err) {
        logger.elog(new Date().toString() +": "+err);
    }

    //esay if debug
    public  final void edsay(String err) {
        if(debug) {
            logger.elog(new Date().toString() +": "+err);
        }
    }

    public abstract void connect() throws Exception;

    public abstract void start() throws Exception;

    public X509Credential getCredential() throws Exception {
        if (configuration.isUseproxy()) {
            return new PEMCredential(configuration.getX509_user_proxy(), (char[]) null);
        } else {
            return new PEMCredential(configuration.getX509_user_key(), configuration.getX509_user_cert(), null);
        }
    }

    private void setReportSuccessStatusBySource(URI url){
        if(report == null) {
            return;
        }
        report.setStatusBySourceUrl(url, Report.OK_RC, null);

    }

    private void setReportSuccessStatusByDest(URI url){
        if(report == null) {
            return;
        }
        report.setStatusByDestinationUrl(url, Report.OK_RC, null);

    }

    private void setReportSuccessStatusBySrcAndDest(URI srcurl, URI dsturl){
        if(srcurl == null ) {
            setReportSuccessStatusByDest(dsturl);
            return;
        }
        if(dsturl == null ) {
            setReportSuccessStatusBySource(srcurl);
            return;
        }

        if(report == null) {
            return;
        }



        report.setStatusBySourceDestinationUrl(srcurl, dsturl, Report.OK_RC, null);
    }

    private void setReportFailedStatusBySource(URI url, String error){
        if(report == null) {
            return;
        }
        if(error == null) {
            report.setStatusBySourceUrl(url, Report.ERROR_RC, "unknown error");
            return;
        }
        error = error.replace('\n', ' ');
        if(error.toLowerCase().contains("file exists")) {
            report.setStatusBySourceUrl(url, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().contains("permission")) {
            report.setStatusBySourceUrl(url, Report.PERMISSION_RC, error);
            return;
        }
        report.setStatusBySourceUrl(url, Report.ERROR_RC, error);
    }

    private void setReportFailedStatusByDest(URI url, String error){
        if(report == null) {
            return;
        }
        if(error == null) {
            report.setStatusByDestinationUrl(url, Report.ERROR_RC, "unknown error");
            return;
        }
        error = error.replace('\n', ' ');
        if(error.toLowerCase().contains("file exists")) {
            report.setStatusByDestinationUrl(url, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().contains("permission")) {
            report.setStatusByDestinationUrl(url, Report.PERMISSION_RC, error);
            return;
        }

        report.setStatusByDestinationUrl(url, Report.ERROR_RC, error);
    }

    private void setReportFailedStatusBySrcAndDest(URI srcurl, URI dsturl, String error){
        if(srcurl == null ) {
            setReportFailedStatusByDest(dsturl,error);
            return;
        }
        if(dsturl == null ) {
            setReportFailedStatusBySource(srcurl,error);
            return;
        }

        if(report == null) {
            return;
        }


        if(error == null) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.ERROR_RC, "unknown error");
            return;
        }
        error = error.replace('\n', ' ');
        if(error.toLowerCase().contains("file exists")) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.FILE_EXISTS_RC, error);
            return;
        }
        if(error.toLowerCase().contains("permission")) {
            report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.PERMISSION_RC, error);
            return;
        }

        report.setStatusBySourceDestinationUrl(srcurl,dsturl, Report.ERROR_RC, error);
    }

    protected void setReportFailed(URI srcurl, URI dsturl,String error ) {
        try {
            setReportFailedStatusBySrcAndDest(srcurl,dsturl, error);
        } catch(Exception e) {
            try {
                setReportFailedStatusByDest(dsturl, error);
            } catch(Exception e1){
                setReportFailedStatusBySource(srcurl,error);
            }
        }
    }

    protected void setReportSucceeded(URI srcurl, URI dsturl) {
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

