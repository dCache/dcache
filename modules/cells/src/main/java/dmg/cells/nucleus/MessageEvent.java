package dmg.cells.nucleus;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;

public class MessageEvent extends CellEvent {

    public MessageEvent(CellMessage msg) {
        super(requireNonNull(msg), CellEvent.OTHER_EVENT);
    }

    @Nonnull
    public CellMessage getMessage() {
        return (CellMessage) getSource();
    }

    public String toString() {
        return "MessageEvent(source=" + getMessage() + ')';
    }
}
