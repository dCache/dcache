package diskCacheV111.util ;
import java.util.* ;
import java.io.PrintWriter ;
import dmg.util.*;
public class HsmSet {
    private Hashtable _hsm = new Hashtable() ;
    public class HsmInfo {
        private String    _name = null ;
        private Hashtable _attr = new Hashtable() ;
        public HsmInfo( String hsmName ){
           _name = hsmName.toLowerCase() ;
        }
        public String getName(){ return _name ; }
        public String getAttribute( String attrName ){
           return (String)_attr.get( attrName ) ;
        }
        public void unsetAttribute( String attrName ){
           _attr.remove( attrName ) ;
        }
        public void setAttribute( String attrName , String attrValue ){
           _attr.put( attrName , attrValue ) ;
        }
        public Enumeration attributes(){ return _attr.keys() ; }
    }
    public Enumeration hsms(){ return _hsm.keys() ; }
    public HsmInfo getHsmInfoByName( String hsmName ){
       return (HsmInfo)_hsm.get(hsmName.toLowerCase()) ;
    }
    public void removeInfo( String hsmName ){
        _hsm.remove( hsmName ) ;
    }
    public HsmInfo createInfo( String hsmName ){
       HsmInfo info = (HsmInfo)_hsm.get(hsmName) ;
       if( info == null )
           _hsm.put( hsmName , info = new HsmInfo( hsmName ) ) ;
       return info ;
    }
    private void _scanOptions( HsmInfo info , Args args ){
       Enumeration e = args.options().keys() ;
       while( e.hasMoreElements() ){
          String optName  = e.nextElement().toString() ;
          String optValue = args.getOpt( optName ) ;
          
          info.setAttribute( optName , optValue == null ? "" : optValue ) ;
       }
    }
    private void _scanOptionsUnset( HsmInfo info , Args args ){
       Enumeration e = args.options().keys() ;
       while( e.hasMoreElements() ){
          String optName  = e.nextElement().toString() ;
          
          info.unsetAttribute( optName  ) ;
       }
    }
    public String hh_hsm_set = "<hsmName> [-<key>=<value>] ... " ;
    public String ac_hsm_set_$_1( Args args ){
       String hsmName = args.argv(0) ;
       HsmInfo info = createInfo( hsmName ) ;
       _scanOptions( info , args ) ;
       return "" ;
    }
    public String hh_hsm_unset = "<hsmName> [-<key>] ... " ;
    public String ac_hsm_unset_$_1( Args args ){
       String hsmName = args.argv(0) ;
       HsmInfo info = getHsmInfoByName( hsmName ) ;
       if( info == null )
          throw new
          IllegalArgumentException( "Hsm not found : "+hsmName ) ;
          
       _scanOptionsUnset( info , args ) ;
       return "" ;
    }
    public String hh_hsm_ls = "[<hsmName>] ..." ;
    public String ac_hsm_ls_$_0_99(Args args ){
       StringBuffer sb = new StringBuffer() ;
       if( args.argc() > 0 ){
          for( int i = 0 ; i < args.argc() ; i++ ){
             _printInfos( sb , args.argv(i) ) ;
          }
       }else{
          Enumeration e = hsms() ;
          while( e.hasMoreElements() ){
             _printInfos( sb , e.nextElement().toString() ) ;
          }
       }
       return sb.toString() ;
    }
    public String hh_hsm_remove = "<hsmName>" ;
    public String ac_hsm_remove_$_1(Args args ){
       removeInfo( args.argv(0) ) ; 
       return "" ;      
    }
    public void printSetup( PrintWriter pw ){
       Enumeration e = _hsm.elements() ;
       while( e.hasMoreElements() ){
       HsmInfo info = (HsmInfo)e.nextElement() ;
       Enumeration f = info.attributes() ;
          while( f.hasMoreElements() ){
              String attrName  = (String)f.nextElement() ;
              String attrValue = info.getAttribute( attrName ) ;
              pw.print( "hsm set ") ;
              pw.print(info.getName()) ;
              pw.print(" -") ;
              pw.print(attrName) ;
              pw.print("=") ;
              pw.println(attrValue==null?"-":attrValue) ;
          }
       
       }
       return ;
    }
    private void _printInfos( StringBuffer sb , String hsmName ){
       HsmInfo info   = getHsmInfoByName( hsmName ) ;
       sb.append( hsmName ) ;
       if( info == null ){
          sb.append("   Not Found\n");
       }else{
          sb.append("\n") ;
       }
       Enumeration f = info.attributes() ;
       while( f.hasMoreElements() ){
           String attrName  = (String)f.nextElement() ;
           String attrValue = info.getAttribute( attrName ) ;
           sb.append( "   " ).
              append(Formats.field(attrName,20,Formats.LEFT)).
              append(attrValue==null?"<set>":attrValue).
              append("\n");
       }
    }
}
