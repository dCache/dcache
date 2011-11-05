package org.dcache.gplazma.configuration.parser;

public class ParseException extends RuntimeException
{
    static final long serialVersionUID = 8146460786081822785L;

    private int offset =-1;

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParseException(String message, int offset) {
        super(message);
        this.offset = offset;
    }

    public ParseException(String message, Throwable cause,int offset) {
        super(message, cause);
        this.offset = offset;
    }

    /**
     * @return the offset where error has occured or -1 if offset is not known
     */
    public int getOffset() {
        return offset;
    }

    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset) {
        if(offset <0) {
            throw new IllegalArgumentException("Illeags Offset: "+offset);
        }
        this.offset = offset;
    }

    @Override
    public String getMessage() {
        String s = super.getMessage();
        if(offset == -1) {
            return s;
        }
        return s+" [offset="+offset+"]";
    }


}
