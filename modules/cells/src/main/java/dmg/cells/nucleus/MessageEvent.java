package dmg.cells.nucleus;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageEvent extends CellEvent
{
    public MessageEvent(CellMessage msg)
    {
        super(checkNotNull(msg), CellEvent.OTHER_EVENT);
    }

    @Nonnull
    public CellMessage getMessage()
    {
        return (CellMessage) getSource();
    }

    public String toString()
    {
        return "MessageEvent(source=" + getMessage() + ")";
    }
}
