package dmg.cells.services.multicaster ;

import java.io.Serializable;

public class MulticastRegister extends MulticastMessage {
    private static final long serialVersionUID = -6863541818189215129L;
    private Object _serverDetail;
    public MulticastRegister( String eventClass , String eventName ){
        super( eventClass , eventName ) ;
    }
    public void setServerInfo( Serializable serverDetail , Serializable message ){
        _serverDetail = serverDetail ;
        setMessage( message ) ;
    }
    public Serializable getServerDetail(){ return (Serializable) _serverDetail ;}
    public String toString(){
      return super.toString()+
             ";detail="+
             (_serverDetail==null?"<none>":_serverDetail.toString()) ;
    }
}
