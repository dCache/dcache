//$Id: PnfsMessage.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.acl.enums.AccessMask;

/**
 * Base class for messages to PnfsManager.
 */
public class PnfsMessage extends Message {

    private PnfsId _pnfsId;
    private String _path;
    private Set<AccessMask> _mask = Collections.emptySet();

    private static final long serialVersionUID = -3686370854772807059L;

    public PnfsMessage(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }

    public PnfsMessage(){ }

    public void setPnfsPath( String pnfsPath ){ _path = pnfsPath ; }
    public String getPnfsPath(){ return _path ;}

    public PnfsId getPnfsId(){
	return _pnfsId;
    }

    public void setPnfsId(PnfsId pnfsId){
	_pnfsId = pnfsId ;
    }

    public void setAccessMask(Set<AccessMask> mask)
    {
        if (mask == null) {
            throw new IllegalArgumentException("Null argument not allowed");
        }
        _mask = mask;
    }

    public Set<AccessMask> getAccessMask()
    {
        return _mask;
    }

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

    /**
     * For compatibility with pre-1.9.6 installations, we fill in the
     * _mask field if it is missing.
     */
    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_mask == null) {
            _mask = Collections.emptySet();
        }
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



