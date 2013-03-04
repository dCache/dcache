package dmg.util.edb ;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public interface JdbmSerializable extends Serializable {

    public static final int BASIC = 0x10 ;
    public static final int BUCKET_ELEMENT   = 0x11 ;
    public static final int BUCKET           = 0x12 ;
    public static final int BUCKET_CONTAINER = 0x13 ;
    public static final int AV_ELEMENT       = 0x14 ;
    public static final int AV_ELEMENT_LIST  = 0x15 ;
    public static final int DIRECTORY        = 0x16 ;
    public static final int FILE_HEADER      = 0x17 ;
    public static final int HEADER_SIZE      = 2 ;
    public void writeObject( ObjectOutput out )
           throws IOException ;
    public void readObject( ObjectInput in )
           throws IOException , ClassNotFoundException ;

    public int getPersistentSize() ;
}
