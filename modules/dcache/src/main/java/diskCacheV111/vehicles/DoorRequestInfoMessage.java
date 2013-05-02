// $Id: DoorRequestInfoMessage.java,v 1.8 2006-04-11 09:47:53 tigran Exp $
package diskCacheV111.vehicles;

import org.antlr.stringtemplate.StringTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.cert.X509Certificate;

import org.dcache.auth.Subjects;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.CertificateUtils;

public class DoorRequestInfoMessage extends PnfsFileInfoMessage
{
    private static final Logger _logger
        = LoggerFactory.getLogger(DoorRequestInfoMessage.class);
    private long _transactionTime = 0;
    private String _client = "unknown";
    private String _owner;

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
        return _owner;
    }

    public void setOwner(String owner) {
        _owner = owner;
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
    public void setSubject(Subject subject) {
        super.setSubject(subject);
        if (_owner == null) {
	     setOwner(getOwner(subject));
        }
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

    private static String getOwner(Subject subject) {
        String owner = Subjects.getDn(subject);
        if (owner == null) {
            owner = findDnFromCredential(subject);
        }
        if (owner == null) {
            owner = Subjects.getUserName(subject);
        }
        return owner;
    }

    private static String findDnFromCredential(Subject subject) {
        for (Object credential: subject.getPublicCredentials()) {
            if (credential instanceof X509Certificate[]) {
                X509Certificate[] chain = (X509Certificate[]) credential;
                try {
                    return CertificateUtils.getSubjectFromX509Chain(chain, false);
                } catch (AuthenticationException t) {
                    _logger.error("There was a problem in extracting the DN from a certificate chain in {}",
                                  subject);
                }
            }
        }
        return null;
    }
}
