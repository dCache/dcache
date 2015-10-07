//$Id: PnfsMessage.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for messages to PnfsManager.
 */
public class PnfsMessage extends Message {

    private PnfsId _pnfsId;
    private String _path;
    private Restriction _restriction = Restrictions.none();

    private Set<AccessMask> _mask = Collections.emptySet();

    private static final long serialVersionUID = -3686370854772807059L;

    public PnfsMessage(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }

    public PnfsMessage(){ }

    public void setPnfsPath( String pnfsPath ){ _path = pnfsPath ; }
    public String getPnfsPath(){ return _path ;}

    public FsPath getFsPath()
    {
        return _path == null ? null : new FsPath(_path);
    }

    public PnfsId getPnfsId(){
	return _pnfsId;
    }

    public void setPnfsId(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }

    public void setAccessMask(Set<AccessMask> mask)
    {
        _mask = checkNotNull(mask);
    }

    public Set<AccessMask> getAccessMask()
    {
        return _mask;
    }

    public void setRestriction(Restriction restriction)
    {
        _restriction = checkNotNull(restriction);
    }

    public Restriction getRestriction()
    {
        return _restriction;
    }

    @Override
    public String toString(){
        return _pnfsId==null?
               (_path==null?"NULL":("Path="+_path)):
               ("PnfsId="+_pnfsId.toString()) ;
    }

    protected final boolean genericInvalidatesForPnfsMessage(Message message)
    {
        /* Conservatively assume that a PnfsMessage invalidates any
         * non PnfsMessage.
         */
        if (!(message instanceof PnfsMessage)) {
            return true;
        }

        PnfsMessage msg = (PnfsMessage) message;

        /* Conservatively assume that this PnfsMessage invalidates
         * another PnfsMessage if we cannot compare them because one
         * message is by path and the other by ID or if either the
         * PNFS IDs or paths are the same.
         */
        return
            ((getPnfsId() == null || msg.getPnfsId() == null) &&
             (getPnfsPath() == null || msg.getPnfsPath() == null)) ||
            (getPnfsId() != null && msg.getPnfsId() != null &&
             getPnfsId().equals(msg.getPnfsId())) ||
            (getPnfsPath() != null && msg.getPnfsPath() != null &&
             getPnfsPath().equals(msg.getPnfsPath()));
    }

    @Override
    public boolean invalidates(Message message)
    {
        return genericInvalidatesForPnfsMessage(message);
    }

    @Override
    public String getDiagnosticContext() {
        String diagnosticContext = super.getDiagnosticContext();
        PnfsId pnfsId = getPnfsId();
        if (pnfsId != null) {
            diagnosticContext = diagnosticContext + " " + pnfsId;
        }
        return diagnosticContext;
    }
}



