
package diskCacheV111.services.space;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import diskCacheV111.util.VOInfo;


public class LinkGroupAuthorizationFile  {

	private Map<String,LinkGroupAuthorizationRecord> records =
            new HashMap<>();

	public LinkGroupAuthorizationFile(String filename)
	    throws IOException,ParseException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
		read(bufferedReader);
		bufferedReader.close();
	}

	public LinkGroupAuthorizationFile(InputStream is)
		throws IOException,ParseException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
		read(bufferedReader);
	}

	public Collection<LinkGroupAuthorizationRecord>  geLinkGroupAuthiorizationRecords() {
		return records.values();
	}

        public LinkGroupAuthorizationRecord getLinkGroupAuthorizationRecord(String LinkGroupName) {
            //records.get
            return records.get(LinkGroupName);
        }

	public void addLinkGroupAuthiorizationRecord(final LinkGroupAuthorizationRecord record) {
		records.put(record.getLinkGroupName(),record);
	}

	public void dump() {
            for (LinkGroupAuthorizationRecord record : records.values()) {
                System.out.println(record.toString());
                System.out.println();
            }
	}

        private static final int OUTSIDE_STATE=0;
        private static final int LINKGROUP_STATE=1;
	private static final String COMMENT_KEY="#";
	private static final String LINKGROUP_KEY="LinkGroup";

	private void read(BufferedReader reader)
		throws IOException, ParseException {
		String line;
                int state = OUTSIDE_STATE;
                String linkGroupName = null;
                List<VOInfo> voinfos = null;
		int icount=0;
		while((line = reader.readLine()) != null) {
			icount++;
			line = line.trim();
			if (line.startsWith(COMMENT_KEY) ) {
				continue;
			}
                        if(state == OUTSIDE_STATE) {
                             if(line.length() == 0) {
                                 continue;
                             }
                            StringTokenizer st = new StringTokenizer(line);
                            if(! LINKGROUP_KEY.equals(st.nextToken()))  {
                                throw new ParseException("line "+icount+
                                    "syntax violation: First token must be "+
                                    LINKGROUP_KEY, icount);
                            }
                            state = LINKGROUP_STATE;
                            linkGroupName = st.nextToken();
                            voinfos = new ArrayList<>();
                        }
                        else if(state == LINKGROUP_STATE ) {
                            if(line.length() == 0) {
                                LinkGroupAuthorizationRecord record =
                                    new LinkGroupAuthorizationRecord(
                                    linkGroupName, voinfos);
                                records.put(linkGroupName,record);
                                state = OUTSIDE_STATE;
                                voinfos =null;
                                linkGroupName = null;
                                continue;
                            }
                            VOInfo voinfo = new VOInfo(line);
                            voinfos.add(voinfo);

                        }
		}

                if(state == LINKGROUP_STATE ) {
                    LinkGroupAuthorizationRecord record =
                        new LinkGroupAuthorizationRecord(
                        linkGroupName, voinfos);
                    records.put(linkGroupName,record);
                }
        }



	public static void main(String[] argv) {
		if (argv.length==1) {
			try {
				LinkGroupAuthorizationFile f = new LinkGroupAuthorizationFile(argv[0]);
                                f.dump();
			}
			catch (IOException e) {
				System.err.println("Failed to open or read file "+argv[0]);
				System.exit(1);
			}
                        catch(ParseException pe) {
				System.err.println("Failed to parse file "+argv[0]);
				System.exit(1);

                        }
		}
		else {
				System.err.println("Usage: java diskCacheV111.services.space.LinkGroupAuthorizationFile <file name>");
				System.exit(1);
		}

	}
}




