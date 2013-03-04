package org.dcache.gplazma.configuration.parser;

import org.dcache.gplazma.GPlazmaInternalException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *  This Exception indicates there was a problem reading the
 *  structure of the gPlazma configuration file.
 */
public class ParseException extends GPlazmaInternalException
{
    private static final long serialVersionUID = 8146460786081822785L;

    private int offset =-1;

    public ParseException(String message)
    {
        super(message);
    }

    public ParseException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ParseException(String message, int offset)
    {
        super(message);
        this.offset = offset;
    }

    public ParseException(String message, Throwable cause, int offset)
    {
        super(message, cause);
        this.offset = offset;
    }

    /**
     * @return the offset where error has occured or -1 if offset is not known
     */
    public int getOffset()
    {
        return offset;
    }

    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset)
    {
        checkArgument(offset >= 0, "invalid offset: %s", offset);
        this.offset = offset;
    }

    @Override
    public String getMessage()
    {
        String s = super.getMessage();
        if(offset == -1) {
            return s;
        }
        return s+" [offset="+offset+"]";
    }
}
