// $Id: DoorRequestInfoMessage.java,v 1.8 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

import org.antlr.stringtemplate.StringTemplate;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Set;

import org.dcache.auth.Subjects;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertificateUtils;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.any;

public class DoorRequestInfoMessage extends PnfsFileInfoMessage
{
    private long _transactionTime = 0;
    private String _client = "unknown";

    private static final Logger _logger
        = LoggerFactory.getLogger(DoorRequestInfoMessage.class);
    private static final long serialVersionUID = 2469895982145157834L;

    public DoorRequestInfoMessage(String cellName) {
        super("request", "door", cellName, null);
    }

    public DoorRequestInfoMessage(String cellName, String action) {
    	//action: "remove"
    	super(action, "door", cellName, null);
     }

    public void setTransactionDuration(long duration) {
        _transactionTime = duration;
    }

    public long getTransactionDuration() {
        return _transactionTime;
    }

    public String toString() {
        return getInfoHeader() + " [" + this.getUserInfo() + "] " + getFileInfo() + " " + _transactionTime
                + " " + getTimeQueued() + " " + getResult();
    }

    public String getClient() {
        return _client;
    }

    public void setClient(String client) {
        _client = client;
    }

    public String getOwner()
    {
        Subject subject = getSubject();
        String owner = Subjects.getDn(subject);
        if (owner == null) {
            owner = Subjects.getUserName(subject);
        }
        return owner;
    }

    public int getGid()
    {
        long[] gids = Subjects.getGids(getSubject());
        return (gids.length > 0) ? (int) gids[0] : -1;
    }

    public int getUid()
    {
        long[] uids = Subjects.getUids(getSubject());
        return (uids.length > 0) ? (int) uids[0] : -1;
    }

    public String getUserInfo()
    {
        return "\"" + getOwner() + "\":" + getUid() + ":" + getGid() + ":" + _client;
    }

    @Override
    public void fillTemplate(StringTemplate template)
    {
        super.fillTemplate(template);
        template.setAttribute("transactionTime", _transactionTime);
        template.setAttribute("uid", getUid());
        template.setAttribute("gid", getGid());
        template.setAttribute("owner", getOwner());
        template.setAttribute("client", getClient());
    }

    @Override
    public void setSubject(Subject subject) {
        checkForDNPrincipal(subject);
        super.setSubject(subject);
    }

    /*
     * Temporary hack for version 2.2.  The modified API in 2.6+ will allow
     * plugins to retain principals in the final Subject which were not
     * necessarily mapped in the authorization phase.
     */
    private static void checkForDNPrincipal(Subject subject) {
        Set<Principal> principals = subject.getPrincipals();
        if (!any(principals, instanceOf(GlobusPrincipal.class))) {
            try {
                CertificateUtils.addGlobusPrincipals(subject.getPublicCredentials(),
                                                     principals);
            } catch (AuthenticationException t) {
                _logger.warn("{} {} {}", new Object[]
                                {"There were problems with an attempted",
                                "extraction of a DN principal from the",
                                "Subject'spublic certificate chains"});
            }
        }
    }
}
