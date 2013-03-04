public class Test {
	private static TpsAPISEI tpsAPISEIPort = null;
	public static void main( String [] args ) {
		try {
            terapathsexamplejavaclient.TpsAPI tpsAPI = new terapathsexamplejavaclient.TpsAPI_Impl();
            tpsAPISEIPort = tpsAPI.getTpsAPISEIPort();
			System.out.println(tpsAPISEIPort);
                    ((javax.xml.rpc.Stub) tpsAPISEIPort)._setProperty(javax.xml.rpc.Stub.ENDPOINT_ADDRESS_PROPERTY, "https://198.124.220.9:48589/terapathsAPI/tpsAPI");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
