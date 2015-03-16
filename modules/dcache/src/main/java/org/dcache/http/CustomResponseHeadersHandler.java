package org.dcache.http;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Support for injecting admin-supplied custom headers into the response headers.
 */
public class CustomResponseHeadersHandler extends ChannelOutboundHandlerAdapter
{
    private final ImmutableMap<String,String> customHeaders;

    public CustomResponseHeadersHandler(ImmutableMap<String,String> customHeaders)
    {
        this.customHeaders = checkNotNull(customHeaders);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object genericMessage, ChannelPromise promise) throws Exception
    {
        if (genericMessage instanceof HttpMessage) {
            HttpHeaders headers = ((HttpMessage) genericMessage).headers();
            customHeaders.forEach(
                    (name,value) -> {
                        if (!headers.contains(name)) {
                            headers.set(name, value);
                        }
                    }
                );
        }
        super.write(ctx, genericMessage, promise);
    }
}
