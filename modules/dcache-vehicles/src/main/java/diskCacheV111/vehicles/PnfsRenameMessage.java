/*
 * $Id: PnfsRenameMessage.java,v 1.2 2005-02-21 15:49:33 tigran Exp $
 */

package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import static java.util.Objects.requireNonNull;

public class PnfsRenameMessage extends PnfsMessage
{
    private static final long serialVersionUID = 4595311595112609899L;

    private final String _newName;
    private final boolean _overwrite;

    public PnfsRenameMessage(PnfsId pnfsId, String newName, boolean overwrite) {
        super(pnfsId);
        _newName = newName;
        _overwrite = overwrite;
        setReplyRequired(true);
    }

    public PnfsRenameMessage(PnfsId pnfsId, String path, String newName, boolean overwrite) {
        super(pnfsId);
        _overwrite = overwrite;
        _newName = requireNonNull(newName);
        setPnfsPath(requireNonNull(path));
        setReplyRequired(true);
    }

    public PnfsRenameMessage(String path, String newName, boolean overwrite)
    {
        this(null, path, newName, overwrite);
    }

    public String newName() {
        return _newName;
    }

    public boolean getOverwrite() {
        return _overwrite;
    }
}
