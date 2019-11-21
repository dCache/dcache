package dmg.cells.nucleus;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public class MessageEvent extends CellEvent
{
    public MessageEvent(CellMessage msg)
    {
        super(requireNonNull(msg), CellEvent.OTHER_EVENT);
    }

    @Nonnull
    public CellMessage getMessage()
    {
        return (CellMessage) getSource();
    }

    public String toString()
    {
        return "MessageEvent(source=" + getMessage() + ')';
    }
}
