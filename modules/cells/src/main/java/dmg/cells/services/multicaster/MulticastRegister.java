package dmg.cells.services.multicaster ;

public class MulticastRegister extends MulticastMessage {
    static final long serialVersionUID = -6863541818189215129L;
    private Object _serverDetail;
    public MulticastRegister( String eventClass , String eventName ){
        super( eventClass , eventName ) ;
    }
    public void setServerInfo( Object serverDetail , Object message ){
        _serverDetail = serverDetail ;
        setMessage( message ) ;
    }
    public Object getServerDetail(){ return _serverDetail ;}
    public String toString(){
      return super.toString()+
             ";detail="+
             (_serverDetail==null?"<none>":_serverDetail.toString()) ;
    }
}
