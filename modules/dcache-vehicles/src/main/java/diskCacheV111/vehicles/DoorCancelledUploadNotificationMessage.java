package diskCacheV111.vehicles;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;

import dmg.cells.services.login.LoginManager;

import static java.util.Objects.requireNonNull;

/**
 * Notify that an upload has been cancelled.  The sender of this Message
 * has ensured that the file represented by the PNFS-ID no longer exists within
 * the namespace.
 */
public class DoorCancelledUploadNotificationMessage extends Message
        implements LoginManager.OfInterestToChildren
{
    private static final long serialVersionUID = 1L;

    private final PnfsId _pnfsId;
    private final String _explanation;

    public DoorCancelledUploadNotificationMessage(Subject subject, PnfsId id,
            String explanation)
    {
        setSubject(subject);
        _pnfsId = requireNonNull(id);
        _explanation = requireNonNull(explanation);
    }

    @Nonnull
    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    @Nonnull
    public String getExplanation()
    {
        return _explanation;
    }
}
