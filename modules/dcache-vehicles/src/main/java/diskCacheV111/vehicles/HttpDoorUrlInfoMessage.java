package diskCacheV111.vehicles;

public class HttpDoorUrlInfoMessage extends Message
{
  private final String pnfsId;
  private final String url;

  private static final long serialVersionUID = 8385138814596693435L;

  public HttpDoorUrlInfoMessage(String pnfsId, String url)
  {
    this.pnfsId = pnfsId;
    this.url = url;
  }

  public String getPnfsId()
  {
    return pnfsId;
  }

  public String getUrl()
  {
    return url;
  }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}


