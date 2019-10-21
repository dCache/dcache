package dmg.cells.nucleus;

import javax.annotation.Nullable;

import java.io.Serializable;

public class NoRouteToCellException extends Exception
{
    private static final long serialVersionUID = -7538969590898439933L;

    private final UOID _uoid;
    private final CellPath _path;
    private final CellMessage _envelope;
    private static final SerializationHandler.Serializer _serializer = SerializationHandler.Serializer.JOS;

    public NoRouteToCellException(CellMessage envelope, String str)
    {
        super(str);
        _envelope = envelope.isStreamMode() ? envelope.ensureEncodedWith(_serializer) : envelope.encodeWith(_serializer);
        _uoid = envelope.getUOID();
        _path = envelope.getDestinationPath();
    }

    @Override
    public String toString()
    {
        return "NoRouteToCell[" + getMessage() + ']';
    }

    @Override
    public String getMessage()
    {
        Serializable messageObject = getMessageObject();
        if (messageObject == null) {
            return "Failed to deliver message " + _uoid + " to " + _path + ": " + super.getMessage();
        } else {
            return "Failed to deliver " + messageObject.getClass().getSimpleName() + " message " + _uoid + " to " + _path + ": " + super.getMessage();
        }
    }

    public UOID getUOID()
    {
        return _uoid;
    }

    public CellPath getDestinationPath()
    {
        return _path;
    }

    @Nullable
    public CellMessage getCellMessage()
    {
        return _envelope != null ? _envelope.decode() : null;
    }

    @Nullable
    public Serializable getMessageObject()
    {
        return _envelope != null ? _envelope.decode().getMessageObject() : null;
    }
}
