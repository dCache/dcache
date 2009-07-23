/*
 * $Id: PnfsRenameMessage.java,v 1.2 2005-02-21 15:49:33 tigran Exp $
 */

package diskCacheV111.vehicles;

import  diskCacheV111.util.* ;

public class PnfsRenameMessage extends PnfsMessage
{
    private String _newName = null;

    private static final long serialVersionUID = 4595311595112609899L;

    public PnfsRenameMessage(PnfsId pnfsId, String newName) {
        super(pnfsId);
        _newName = newName;
        setReplyRequired(true);
    }

    public String newName() {
        return _newName;
    }
}
