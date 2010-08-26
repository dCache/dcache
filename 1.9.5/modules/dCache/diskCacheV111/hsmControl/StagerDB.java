// $Id: StagerDB.java,v 1.2 2007-06-23 22:05:22 tigran Exp $
package diskCacheV111.hsmControl ;

import java.util.Date;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import diskCacheV111.vehicles.StagerMessage;

class StagerDB {

	private final SortedSet<StagerMessage> _store = new TreeSet<StagerMessage>( new DB_Comparator() );
	
	public StagerDB() {
 				
	}
	
	public synchronized void insert(StagerMessage msg) {
		Date date = new Date();
		
		long now = date.getTime() / 1000;
		msg.setStageTime(msg.getStageTime() + now);
		_store.add(msg);
		notifyAll();
	}
	
	
	public synchronized StagerMessage[] nextSet(long time) {
	
		SortedSet<StagerMessage> ss = null;
		Date    date = null;
		
		do {

			try {
			
				while( _store.isEmpty() ) {
					wait();
				}


				date = new Date();
			
				StagerMessage msg = _store.first();
				long now = date.getTime() / 1000;
				try {
					wait(msg.getStageTime() - now ); 
				}catch (Exception e) {
					continue;
				}

				msg.setStageTime(msg.getStageTime()+time);
				ss = _store.headSet(msg);				
				for(StagerMessage message: ss){
					_store.remove(message);
				}

			} catch  (Exception e) {}

		} while(!ss.isEmpty());	
		return ss.toArray(new StagerMessage[ss.size()]);
	}
}
