package org.dcache.services.httpd.util;

import org.eclipse.jetty.server.Handler;

/**
 * Abstraction for httpd aliases.
 *
 * @author arossi
 */
public class AliasEntry
{
    public enum AliasType
    {
        FILE("file"),
        DIR("directory"),
        CONTEXT("context"),
        REDIRECT("redirect"),
        ENGINE("class"),
        WEBAPP("webapp"),
        BADCONFIG("badconfig");

        private final String type;

        AliasType(String type)
        {
            this.type = type;
        }

        public boolean isType(String type)
        {
            return this.type.equalsIgnoreCase(type);
        }

        public String getType()
        {
            return type;
        }

        public static AliasType fromType(String type)
        {
            for (AliasType aliasType : AliasType.values()) {
                if (aliasType.isType(type)) {
                    return aliasType;
                }
            }
            throw new IllegalArgumentException("Unknown alias type: " + type);
        }
    }

    private final String name;
    private final AliasType type;
    private final Handler handler;
    private final String spec;

    private String onError;
    private String overwrite;
    private String intFailureMsg;
    private String statusMessage;

    public AliasEntry(String name, AliasType type, Handler handler, String spec) {
        this.name = name;
        this.type = type;
        this.handler = handler;
        this.spec = spec;
    }

    public void setStatusMessage(String statusMessage)
    {
        this.statusMessage = statusMessage;
    }

    public Handler getHandler() {
        return handler;
    }

    public String getIntFailureMsg() {
        return intFailureMsg;
    }

    public String getName() {
        return name;
    }

    public String getOnError() {
        return onError;
    }

    public String getOverwrite() {
        return overwrite;
    }

    public String getSpecificString() {
        return spec;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public AliasType getType() {
        return type;
    }

    public void setIntFailureMsg(String entry) {
        intFailureMsg = entry;
    }

    public void setOnError(String entry) {
        onError = entry;
    }

    public void setOverwrite(String entry) {
        overwrite = entry;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.getType()).append("(").append(spec).append(")");
        if (onError != null) {
            sb.append(" [onError=").append(onError).append("]");
        }
        if (overwrite != null) {
            sb.append(" [overwrite ").append(overwrite).append("]");
        }
        return sb.toString();
    }
}
