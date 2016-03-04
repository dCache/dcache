package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class EnstoreLocationExtractor implements HsmLocation {


	private final URI _uri;
	/*
	   enstore://enstore/?volume=VOLUME&location=LOCATION&size=SIZE&origff=FAMILY
                      &origname=NAME&mapfile=MAP&pnfsid=PNFSID&pnfsidmap=PNFSIDMAP&bfid=BFID&drive=DRIVE&crc=CRC
     */
	public EnstoreLocationExtractor(URI location) {

		_uri = location;
	}

	public EnstoreLocationExtractor(Map<Integer, String> levels) {


		String storageInfo = levels.get(4);

		if(storageInfo == null ) {
			throw new IllegalArgumentException("Enstore uses level 1 and 4");
		}

		 StringBuilder sb = new StringBuilder("enstore://enstore/?");
		 BufferedReader br = new BufferedReader(  new StringReader(storageInfo)  ) ;

         try{
        	 String line;
             for( int i =  0 ; ; i++ ){
                 try{
                     if( ( line = br.readLine() ) == null ) {
                         break;
                     }
                 }catch(IOException ioe ){
                     break ;
                 }
                 switch(i){
                    case 0 : sb.append("volume=").append(line).append("&");  break ;
                    case 1 : sb.append("location=").append(line).append("&");  break ;
                    case 2 : sb.append("size=").append(line).append("&");  break ;
                    case 3 : sb.append("origff=").append(line).append("&");  break ;
                    case 4 : sb.append("origname=").append(line).append("&");  break ;
                    case 5 : sb.append("mapfile=").append(line).append("&");  break ;
                    case 6 : sb.append("pnfsid=").append(line).append("&");  break ;
                    case 7 : sb.append("pnfsidmap=").append(line).append("&");  break ;
                    case 8 : sb.append("bfid=").append(line).append("&");  break ;
                    case 9 : sb.append("drive=").append(line).append("&");  break ;
                    case 10 : sb.append("crc=").append(line);  break ;
                 }
             }
          }finally{
             try{ br.close() ; }catch(Exception ie ){}
          }


		try {
			_uri = new URI(sb.toString());
		} catch (URISyntaxException e) {
			//should never happen, but nevertheless
			throw new IllegalArgumentException("failed to generate URI from level: "  + storageInfo) ;
		}
	}


	@Override
        public URI location() {
		return _uri;
	}


	@Override
        public Map<Integer, String> toLevels() {

		Map<Integer, String> levelData = new HashMap<>(2);

		Map<String, String> parsed = parseURI(_uri);

		levelData.put(1, parsed.get("bfid") + "\n");

		String asLevel4 = parsed.get("volume") + "\n" +
						  parsed.get("location") + "\n" +
						  parsed.get("size") + "\n" +
						  parsed.get("origff") + "\n" +
						  parsed.get("origname") + "\n" +
						  parsed.get("mapfile") + "\n" +
						  parsed.get("pnfsid") + "\n" +
						  parsed.get("pnfsidmap") + "\n" +
						  parsed.get("bfid") + "\n" +
						  parsed.get("drive") + "\n" +
						  parsed.get("crc") + "\n";

		levelData.put(4, asLevel4);


		return levelData;
	}


	private static Map<String, String> parseURI(URI location) throws IllegalArgumentException {


		Map<String,String> values = new HashMap<>();



		String query = location.getQuery();
		String[] storageInfo = query.split("&");

		if(storageInfo.length != 11) {
			throw new IllegalArgumentException("Invalid URI format (11 fileds expected): " + location);
		}


		for( String s: storageInfo) {
			String[] ss = s.split("=");
			if(ss.length != 2 ) {
				values.put(ss[0], "");
			}else{
				values.put(ss[0], ss[1]);
			}
		}

		return values;
	}
}
