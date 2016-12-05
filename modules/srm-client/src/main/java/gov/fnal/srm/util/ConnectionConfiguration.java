package gov.fnal.srm.util;

/**
 *  Configuration specifically about the connection with the server.
 */
public class ConnectionConfiguration
{
    @Option(name="use_proxy", description = "use user proxy(true) or use certificates directly(false)",
            defaultValue="true", required=false, log=true, save=true)
    private boolean useproxy;

    public boolean isUseproxy()
    {
        return useproxy;
    }

    public void setUseproxy(boolean useproxy)
    {
        this.useproxy = useproxy;
    }

    @Option(name="x509_user_proxy", description="absolute path to user proxy",
            required=false, log=true, save=true)
    private String x509_user_proxy;

    public String getX509_user_proxy()
    {
        return x509_user_proxy;
    }

    public void setX509_user_proxy(String x509_user_proxy)
    {
        this.x509_user_proxy = x509_user_proxy;
    }

    @Option(name="x509_user_cert", description="absolute path to user (or host) certificate",
            required=false, log=true, save=true)
    private String x509_user_cert;

    public String getX509_user_cert()
    {
        return x509_user_cert;
    }

    public void setX509_user_cert(String x509_user_cert)
    {
        this.x509_user_cert = x509_user_cert;
    }

    @Option(name="x509_user_key", description="absolute path to user (or host) private key",
            required=false, log=true, save=true)
    private String x509_user_key;

    public String getX509_user_key()
    {
        return x509_user_key;
    }

    public void setX509_user_key(String x509_user_key)
    {
        this.x509_user_key = x509_user_key;
    }

    @Option(name="x509_user_trusted_certificates", description="absolute path to the trusted certificates directory",
            defaultValue="/etc/grid-security/certificates", required=false,
            log=true, save=true)
    private String x509_user_trusted_certificates;

    public String getX509_user_trusted_certificates()
    {
        return x509_user_trusted_certificates;
    }

    public void setX509_user_trusted_certificates(String x509_user_trusted_certificates)
    {
        this.x509_user_trusted_certificates = x509_user_trusted_certificates;
    }

    @Option(name="gss_expected_name", description="gss expected name",
            required=false, log=true, save=true)
    private String gss_expected_name;

    public String getGss_expected_name() {
        if (gss_expected_name == null){
            gss_expected_name = "host";
        }
        return gss_expected_name;
    }

    public void setGss_expected_name(String gss_expected_name) {
        this.gss_expected_name = gss_expected_name;
    }

    @Option(name = "retry_timeout", description="number of miliseconds to sleep after a failure",
            defaultValue="10000", unit="milliseconds", required=false, log=true,
            save=true)
    private long retry_timeout;

    public long getRetry_timeout()
    {
        return retry_timeout;
    }

    public void setRetry_timeout(long retry_timeout)
    {
        this.retry_timeout = retry_timeout;
    }

    @Option(name="retry_num", description="number of retries before client gives up",
            defaultValue="20", required=false, log=true, save=true)
    private int retry_num;

    public int getRetry_num()
    {
        return retry_num;
    }

    public void setRetry_num(int retry_num)
    {
        this.retry_num = retry_num;
    }

    @Option(name="delegate", description="enables delegation of user credenital to the server",
            defaultValue="false", required=false, log=true, save=true)
    private boolean delegate;

    public boolean isDelegate()
    {
        return delegate;
    }

    public void setDelegate(boolean delegate)
    {
        this.delegate = delegate;
    }

    @Option(name="full_delegation", description="specifies type (full or limited) of delegation",
            defaultValue="true", required=false, log=true, save=true)
    private boolean full_delegation;

    public boolean isFull_delegation()
    {
        return full_delegation;
    }

    public void setFull_delegation(boolean full_delegation)
    {
        this.full_delegation = full_delegation;
    }
}
