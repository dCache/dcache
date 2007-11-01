// $Id: StagerDB.java,v 1.1 2002-05-06 09:00:52 cvs Exp $
package diskCacheV111.hsmControl ;

import java.util.* ;

import diskCacheV111.vehicles.* ;
import diskCacheV111.util.* ;

class StagerDB {

	private SortedSet _store;
	
	public StagerDB() {
		_store = new TreeSet( new DB_Comparator() );				
	}
	
	public synchronized void insert(StagerMessage msg) {
		Date date = new Date();
		
		long now = date.getTime() / 1000;
		msg.setStageTime(msg.getStageTime() + now);
		_store.add(msg);
		notifyAll();
	}
	
	
	public synchronized StagerMessage[] nextSet(long time) {
	
		SortedSet ss = null;
		Date    date = null;
		
		do {

			try {
			
				while( _store.isEmpty() ) {
					wait();
				}


				date = new Date();
			
				StagerMessage msg = (StagerMessage)_store.first();
				long now = date.getTime() / 1000;
				try {
					wait(msg.getStageTime() - now ); 
				}catch (Exception e) {
					continue;
				}

				msg.setStageTime(msg.getStageTime()+time);
				ss = _store.headSet(msg);
				Iterator I = ss.iterator();
				while(I.hasNext()){
					_store.remove(I.next());
				}

			} catch  (Exception e) {}

		} while(!ss.isEmpty());	
		return (StagerMessage[])ss.toArray();
	}
}
