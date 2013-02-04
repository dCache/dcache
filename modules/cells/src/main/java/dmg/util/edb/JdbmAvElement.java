package dmg.util.edb ;

import java.io.* ;

public class JdbmAvElement implements JdbmSerializable {
    private static final long serialVersionUID = -7313548868280129848L;
    private int     _size;
    private long    _addr;
    public JdbmAvElement(){}
    public JdbmAvElement( long addr , int size ){
       _addr = addr ;
       _size = size ;
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws IOException {
       out.writeInt(_size) ;
       out.writeLong(_addr) ;
    }
    @Override
    public void readObject( ObjectInput in )
           throws IOException, ClassNotFoundException {
           
           
       _size = in.readInt() ;
       _addr = in.readLong() ;

    }
    @Override
    public int getPersistentSize() { return 0 ; }

}
