package org.dcache.services.pinmanager1;

import java.util.Collection;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import org.dcache.pool.repository.StickyRecord;

/**
 * Request to pin manager to move pins from one pool to another.
 *
 * Since it is really up to the pin manager to decide where to pin a
 * file, this message is to be considered a weak request. The request
 * is considered successful if at the end of the operation the file is
 * no longer pinned on the specified source pool. Whether the file was
 * actually pinned on that pool in the first place or pinned at all is
 * not important.
 *
 * It is not important whether the file is pinned on the target pool
 * after the operation. The target pool is only to be considered a
 * hint about where to move the pin.
 *
 * The list of sticky records is also just a hint. The pin manager
 * should assume that those sticky records actually exist on the
 * source pool and attempt to clear them. If the pin manager has
 * knowledge about any other sticky records on the source pool, then
 * it should remove those too.
 */
public class PinManagerMovePinMessage extends Message
{
    private static final long serialVersionUID = -2917605511586582763L;
    private final PnfsId _pnfsId;
    private final Collection<StickyRecord> _records;
    private final String _sourcePool;
    private final String _targetPool;

    public PinManagerMovePinMessage(PnfsId pnfsId,
                                    Collection<StickyRecord> records,
                                    String sourcePool, String targetPool)
    {
        _pnfsId = pnfsId;
        _records = records;
        _sourcePool = sourcePool;
        _targetPool = targetPool;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public Collection<StickyRecord> getRecords()
    {
        return _records;
    }

    public String getSourcePool()
    {
        return _sourcePool;
    }

    public String getTargetPool()
    {
        return _targetPool;
    }

    @Override
    public String getDiagnosticContext()
    {
        return super.getDiagnosticContext() + " " + getPnfsId();
    }

}
