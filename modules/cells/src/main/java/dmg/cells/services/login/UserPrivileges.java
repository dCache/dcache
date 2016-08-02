package dmg.cells.services.login ;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import dmg.util.Formats;


public class UserPrivileges {
   private Hashtable<String,String> _allowed = new Hashtable<>() ;
   private Hashtable<String,String> _denied  = new Hashtable<>() ;
   private String    _userName = "unknown" ;
   private boolean   _faked;
   UserPrivileges(){}
   UserPrivileges( String userName ){ _userName = userName ; _faked = true ; }
   UserPrivileges( String userName ,
                   String [] allowedList ,
                   String [] deniedList     ){

        _userName = userName ;
       for (String s : allowedList) {
           _allowed.put(s, s);
       }
       for (String s : deniedList) {
           _denied.put(s, s);
           _allowed.remove(s);
       }

   }
   public String getUserName(){ return _userName ; }
   void mergeHorizontal( UserPrivileges right ){
       String attr;

       for (Object o : right._denied.keySet()) {
           _denied.put(attr = (String) o, attr);
       }
       for (Object o : right._allowed.keySet()) {
           _allowed.put(attr = (String) o, attr);
           _denied.remove(attr);
       }
   }
   void mergeVertical( UserPrivileges upper ){
       String attr;

       for (Object o : upper._allowed.keySet()) {
           if (_denied.get((attr = (String) o)) == null) {
               _allowed.put(attr, attr);
           }
       }
       for (Object o : upper._denied.keySet()) {
           if (_allowed.get((attr = (String) o)) == null) {
               _denied.put(attr, attr);
           }
       }
   }
   public boolean isAllowed( String check ){
    if( _faked ) {
        return false;
    }
    if( _userName.equals("root") ) {
        return true;
    }
    try{
       if( _denied.get( check ) != null ) {
           return false;
       }
       if( _allowed.get( check ) != null ) {
           return true;
       }
       String base;
       int last = check.lastIndexOf(':') ;
       if( last < 0 ){
         base  = "" ;
       }else{
         base  = check.substring(0,last) + ':';
         check = check.substring(last+1) ;
       }
       if( check.length() < 1 ) {
           return false;
       }

       StringTokenizer st = new StringTokenizer(check,".") ;
       String [] tokens = new String[st.countTokens()] ;

       for( int i = 0  ; i < tokens.length ; i++ ) {
           tokens[i] = st.nextToken();
       }

       for( int i = tokens.length  ; i > 0 ; i-- ){
          StringBuilder sb = new StringBuilder() ;
          sb.append( base ) ;
          for( int j = 0 ; j < (i-1) ; j++ ) {
              sb.append(tokens[j]).append('.');
          }
          sb.append('*') ;
          String x = sb.toString() ;
          if( _denied.get( x ) != null ) {
              return false;
          }
          if( _allowed.get( x ) != null ) {
              return true;
          }
       }
       return false ;
      }catch( Exception ee ){
         ee.printStackTrace() ;
         return false ;
      }
   }
   public String toString(){
       if( _faked ) {
           return "UserPrivileges for " + _userName + " faked";
       }
       StringBuilder sb = new StringBuilder() ;
       String x = "         " ;
       int dx = 20 ;
       int m = Math.min( _allowed.size() , _denied.size() ) ;
       Iterator<String> a = _allowed.keySet().iterator();
       Iterator<String> d = _denied.keySet().iterator();
       for( int i = 0 ; i < m ; i ++ ) {
           sb.append(x).
                   append(Formats.field(a.next(), dx)).
                   append(Formats.field(d.next(), dx)).
                   append('\n');
       }
       if( _allowed.size() > m ) {
           while (a.hasNext()) {
               sb.append(x).
                       append(Formats.field(a.next(), dx)).
                       append(Formats.field("", 20)).
                       append('\n');
           }
       }
       if( _denied.size() > m ) {
           while (d.hasNext()) {
               sb.append(x).
                       append(Formats.field("", 20)).
                       append(Formats.field(d.next(), dx)).
                       append('\n');
           }
       }

       return sb.toString() ;
   }

}
