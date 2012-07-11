package diskCacheV111.util;

/*
 * $Id: UptimeParser.java,v 1.2 2005-11-24 15:43:38 tigran Exp $
 */

public class UptimeParser {


    private UptimeParser() {
        // no instance allowed
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println( "Uptime: " + UptimeParser.valueOf( 54564 ) );
    }


    /**
     * @param uptime time in seconds
     * @return formated String Object like, "1 day 15:24:31"
     */


    public static String valueOf(long uptime) {

        long days = 86400; // 24*60*60
        long rdays;
        long hour = 3600; // 60*60
        long rhour;
        long min = 60;
        long rmin;
        long sec;
        long tmp = uptime;

        StringBuffer sb = new StringBuffer();

        rdays = tmp/days;
        tmp -= rdays*days;

        rhour = tmp/hour;
        tmp -= rhour*hour;
        rmin = tmp/min;
        tmp -= rmin*min;
        sec = tmp;

        if( rdays != 0 ) {
            sb.append(rdays);
            if( rdays > 1 ){
                sb.append("days ");
            }else{
                sb.append("day ");
            }
        }

        if(rhour < 10) {
            sb.append("0");
        }
        sb.append(rhour).append(":");
        if(rmin < 10) {
            sb.append("0");
        }
        sb.append(rmin).append(":");
        if(sec < 10) {
            sb.append("0");
        }
        sb.append(sec);

        return sb.toString();
    }
}
