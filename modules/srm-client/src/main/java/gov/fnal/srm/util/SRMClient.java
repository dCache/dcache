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

import java.io.IOException;
import java.net.URI;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.Optional;

import org.dcache.srm.Logger;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.client.Transport;
import org.dcache.srm.client.TransportUtil;
import org.dcache.srm.v2_2.ISRM;
import org.dcache.util.URIs;

import static java.util.Objects.requireNonNull;
import static org.dcache.srm.util.Credentials.checkValid;

/**
 *
 * @author  timur
 */
public abstract class SRMClient
{
    protected final Configuration configuration;
    protected final Logger logger;

    protected Report report;
    private Optional<X509Credential> cred;
    protected ISRM srm;

    public SRMClient(Configuration configuration)
    {
        this.configuration = configuration;
        logger = configuration.getLogger();

        Transport transport = configuration.getTransport();
        dsay("In SRMClient ExpectedName: "+configuration.getGss_expected_name());
        dsay("SRMClient("+TransportUtil.uriSchemaFor(transport)+","+transport.toString()+")");
    }

    public final void say(String msg) {
        logger.log(new Date().toString() +": "+msg);
    }

    //say if debug
    public  final void dsay(String msg) {
        if (configuration.isDebug()) {
            logger.log(new Date().toString() +": "+msg);
        }
    }

    //error say
    public final void esay(String err) {
        logger.elog(new Date().toString() +": "+err);
    }

    //esay if debug
    public  final void edsay(String err) {
        if (configuration.isDebug()) {
            logger.elog(new Date().toString() +": "+err);
        }
    }

    /**
     * Provide server URL: enforcing default port number is not necessary.
     */
    protected URI getServerUrl()
    {
        return requireNonNull(configuration.getSrmUrl(), "Must specify SRM URL");
    }

    public void connect() throws Exception
    {
        java.net.URI uri = URIs.withDefaultPort(getServerUrl(), "srm",
                configuration.getDefaultSrmPortNumber());

        srm = new SRMClientV2(uri,
                              getCredential(),
                              getBearerToken(),
                              configuration.getRetry_timeout(),
                              configuration.getRetry_num(),
                              configuration.isDelegate(),
                              configuration.isFull_delegation(),
                              configuration.getGss_expected_name(),
                              configuration.getWebservice_path(),
                              configuration.getX509_user_trusted_certificates(),
                              configuration.getTransport());
    }

    public abstract void start() throws Exception;

    private Optional<X509Credential> getCredential() throws IOException, KeyStoreException,
            CertificateException
    {
        if (cred == null) {
            if (configuration.isUseproxy()) {
                cred = configuration.getX509_user_proxy() == null
                        ? Optional.<X509Credential>empty()
                        : Optional.of(new PEMCredential(configuration.getX509_user_proxy(), (char[]) null));
            } else {
                cred = configuration.getX509_user_key() == null || configuration.getX509_user_cert() == null
                        ? Optional.<X509Credential>empty()
                        : Optional.of(new PEMCredential(configuration.getX509_user_key(), configuration.getX509_user_cert(), null));
            }
        }

        return cred;
    }

    protected void checkCredentialValid() throws IOException
    {
        if (cred != null) {
            checkValid(cred);
        }
    }

    public Optional<String> getBearerToken()
    {
        return Optional.ofNullable(configuration.getBearerToken());
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

