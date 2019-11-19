/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract
No. DE-AC02-76CH03000. Therefore, the U.S. Government retains a
world-wide non-exclusive, royalty-free license to publish or reproduce
these documents and software for U.S. Government purposes.  All
documents and software available from this server are protected under
the U.S. and Foreign Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


Neither the name of Fermilab, the  URA, nor the names of the
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

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
cost, or damages arising out of or resulting from the use of the
software available from this server.


Export Control:

All documents and software available from this server are subject to
U.S. export control laws.  Anyone downloading information from this
server is obligated to secure any necessary Government licenses before
exporting documents or software obtained from this server.
 */
package diskCacheV111.srm;

import eu.emi.security.authn.x509.X509Credential;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.KeyStoreException;
import java.security.cert.CertPath;
import java.security.cert.CertificateEncodingException;
import java.util.Objects;

import diskCacheV111.srm.dcache.DcacheUserManager;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.auth.FQAN;
import org.dcache.auth.LoginReply;
import org.dcache.auth.Subjects;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SrmRequest;
import org.dcache.srm.SrmResponse;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMNotSupportedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.handler.CredentialAwareHandler;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * SRM 2.2 backend message processor.
 *
 * Receives requests from SRM frontends.
 */
public class SrmService implements CellMessageReceiver, CuratorFrameworkAware, CellIdentityAware, CellLifeCycleAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SrmService.class);

    private SRM srm;
    private AbstractStorageElement storage;
    private RequestCredentialStorage requestCredentialStorage;
    private DcacheUserManager userManager;
    private CuratorFramework client;
    private PersistentNode node;
    private CellAddressCore address;
    private String id;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
    }

    @Required
    public void setSrmId(String id)
    {
        this.id = checkNotNull(id);
    }

    @Required
    public void setStorage(AbstractStorageElement storage)
    {
        this.storage = storage;
    }

    @Required
    public void setSrm(SRM srm)
    {
        this.srm = srm;
    }

    @Required
    public void setRequestCredentialStorage(RequestCredentialStorage requestCredentialStorage)
    {
        this.requestCredentialStorage = requestCredentialStorage;
    }

    @Required
    public void setUserManager(DcacheUserManager userManager)
    {
        this.userManager = userManager;
    }

    @Override
    public void afterStart()
    {
        String path = getZooKeeperBackendPath(this.id);
        byte[] data =  address.toString().getBytes(US_ASCII);
        node = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, data);
        node.start();
    }

    @Override
    public void beforeStop()
    {
        if (node != null) {
            CloseableUtils.closeQuietly(node);
        }
    }

    public SrmResponse messageArrived(SrmRequest request) throws SRMException
    {
        try {
            CertPath certPath = getFirst(request.getSubject().getPublicCredentials(CertPath.class), null);
            LoginReply login = new LoginReply(request.getSubject(), request.getLoginAttributes());
            SRMUser user = userManager.persist(certPath, login);

            String requestName = request.getRequestName();
            Class<?> requestClass = request.getRequest().getClass();
            String capitalizedRequestName =
                    Character.toUpperCase(requestName.charAt(0))+
                    requestName.substring(1);
            LOGGER.debug("About to call {} handler", requestName);
            Constructor<?> handlerConstructor;
            Object handler;
            Method handleGetResponseMethod;

            try {
                Class<?> handlerClass = Class.forName("org.dcache.srm.handler." + capitalizedRequestName);
                handlerConstructor =
                        handlerClass.getConstructor(SRMUser.class,
                        requestClass,
                        AbstractStorageElement.class,
                        SRM.class,
                        String.class);
                handler = handlerConstructor.newInstance(user,
                                                         request.getRequest(),
                                                         storage,
                                                         srm,
                                                         request.getRemoteHost());

                if (handler instanceof CredentialAwareHandler) {
                    CredentialAwareHandler credentialAware = (CredentialAwareHandler) handler;
                    RequestCredential requestCredential =
                            saveRequestCredential(request.getSubject(), request.getCredential());
                    credentialAware.setCredential(requestCredential);
                }

                handleGetResponseMethod = handlerClass.getMethod("getResponse");
            } catch (ClassNotFoundException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("handler discovery and dynamic loading failed", e);
                } else {
                    LOGGER.info("handler discovery and dynamic loading failed");
                }
                throw new SRMNotSupportedException(requestName + " is unsupported");
            }
            Object result = handleGetResponseMethod.invoke(handler);
            return new SrmResponse(id, result);
        } catch (CertificateEncodingException | KeyStoreException e) {
            throw new SRMInternalErrorException("Failed to process certificate chain.", e);
        } catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException | RuntimeException e) {
            LOGGER.error("Please report this failure to support@dcache.org", e);
            throw new SRMInternalErrorException("Internal error (server log contains additional information)");
        }
    }

    private RequestCredential saveRequestCredential(Subject subject, X509Credential credential)
    {
        String dn = Subjects.getDn(subject);
        FQAN fqan = Subjects.getPrimaryFqan(subject);
        RequestCredential requestCredential =
                RequestCredential.newRequestCredential(dn, Objects.toString(fqan, null), requestCredentialStorage);
        requestCredential.keepBestDelegatedCredential(credential);
        requestCredential.saveCredential();
        return requestCredential;
    }

    public static String getZooKeeperBackendPath(String id)
    {
        return ZKPaths.makePath("/dcache/srm/backends", id);
    }
}
