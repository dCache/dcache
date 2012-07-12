package dmg.util.edb ;

import java.io.* ;
public class JdbmBasic implements JdbmSerializable {

    private String _string = "" ;
    private JdbmBasic _root = null ;
    public JdbmBasic(){}
    public JdbmBasic( String s ){ 
       int n = s.indexOf(":") ;
       if( n < 0 ){
           _root = null ;
           _string = s ;
       }else{
           _root = new JdbmBasic( s.substring(n+1) ) ;
           _string = s.substring(0,n) ; 
       }
    }
    @Override
    public void writeObject( ObjectOutput out )
           throws java.io.IOException {
           
       out.writeUTF( _string ) ;
       if( _root == null ){
          out.writeInt(0) ;
          return ;
       }else{
          out.writeInt(1) ;
          out.writeObject( _root ) ;
       }
       return ;   
    }
    @Override
    public void readObject( ObjectInput in )
           throws java.io.IOException, ClassNotFoundException {
           
       _string = in.readUTF() ;
       int flag = in.readInt() ;
       System.out.println( "Got="+_string+":"+flag) ;
       if( flag ==  0 ) {
           _root = null;
       } else {
           _root = (JdbmBasic) in.readObject();
       }
    }
    @Override
    public int getPersistentSize() { return 0 ; }
    public String toString(){ 
       if( _root == null ) {
           return _string;
       } else {
           return _string + ":" + _root.toString();
       }
    }
    
    public static void main( String [] args )throws Exception {
        if( args.length == 0 ){
            JdbmBasic jdbm = new JdbmBasic("Otto") ;
            JdbmObjectOutputStream out =
               new JdbmObjectOutputStream(
                   new DataOutputStream(
                       new FileOutputStream( "xxx" ) ) ) ;
            out.writeObject( new JdbmBasic( "otto:karl:waste" ) ) ;
            out.close() ;
        }else {
            JdbmObjectInputStream in = 
              new JdbmObjectInputStream(
                  new DataInputStream(
                      new FileInputStream("xxx") ) ) ;
            JdbmBasic jdbm = null ;
            while( true ){
               try{
                 jdbm = (JdbmBasic) in.readObject() ;
                 System.out.println( jdbm.toString() ) ;
               }catch(IOException ee ){
                 break ;
               } 
            }
            in.close() ;   
        }
    }
}
