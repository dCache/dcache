package dmg.util.edb ;

import java.io.* ;

public class JdbmAvElement implements JdbmSerializable {
    private int     _size;
    private long    _addr;
    public JdbmAvElement(){}
    public JdbmAvElement( long addr , int size ){
       _addr = addr ;
       _size = size ;
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws java.io.IOException {
       out.writeInt(_size) ;
       out.writeLong(_addr) ;
       return ;   
    }
    @Override
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
           
           
       _size = in.readInt() ;
       _addr = in.readLong() ;
       
       return ;
    }
    @Override
    public int getPersistentSize() { return 0 ; }

}
