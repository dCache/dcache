package diskCacheV111.vehicles ;


import java.util.Hashtable ;

public class HpssStorageInfo extends GenericStorageInfo {

   private static final long serialVersionUID = 4260226401319935542L;

   private String _store;
   private String _group;

   public HpssStorageInfo( String store , String group ){
      setHsm("hpss");
      _store = store ;
      _group = group ;
      setIsNew(true) ;

   }
   public HpssStorageInfo( String store , String group , String bfid ){
       setHsm("hpss");
      _store = store ;
      _group = group ;
      setBitfileId(bfid) ;
      setIsNew(false) ;

   }
   @Override
   public String getStorageClass() {
      return (_store==null?"<Unknown>":_store)+":"+
             (_group==null?"<Unknown>":_group) ;
   }
   public String getStore(){ return _store ; }
   public String getStorageGroup(){ return _group ; }
   @Override
   public String getKey( String key ){
      if( key.equals("store") ) {
          return _store;
      } else if( key.equals("group") ) {
          return _group;
      } else {
          return super.getKey(key);
      }
   }
   public String toString(){
      return super.toString()+
             "store="+(_store==null?"<Unknown>":_store)+
             ";group="+(_group==null?"<Unknown>":_group)+
             ";bfid="+getBitfileId()+
             ";" ;

   }
}

