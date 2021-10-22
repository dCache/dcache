/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma.oidc;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.dcache.gplazma.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A TokenProcessor that tries a list of other TokenProcessor until one of them has successfully
 * extracted claims or throws AuthenticationException.  It uses a builder pattern to make the
 * code easier to read.
 */
public class ChainedTokenProcessor implements TokenProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChainedTokenProcessor.class);
    private final List<TokenProcessor> processors;

    public static ChainedTokenProcessor tryWith(TokenProcessor processor)
    {
        return new ChainedTokenProcessor(processor);
    }

    public ChainedTokenProcessor(TokenProcessor processor)
    {
        processors = ImmutableList.of(processor);
    }

    private ChainedTokenProcessor(ChainedTokenProcessor predecessor, TokenProcessor lastProcessor)
    {
        var builder = ImmutableList.<TokenProcessor>builder();
        builder.addAll(predecessor.processors);
        builder.add(lastProcessor);
        processors = builder.build();
    }

    public ChainedTokenProcessor andThenTryWith(TokenProcessor processor) {
        return new ChainedTokenProcessor(this, processor);
    }

    @Override
    public void shutdown() {
        for (TokenProcessor processor : processors) {
            try {
                processor.shutdown();
            } catch (RuntimeException e) {
                LOGGER.warn("Error detected; please report this to <support@dcache.org>", e);
            }
        }
    }

    @Override
    public ExtractResult extract(String token) throws AuthenticationException, UnableToProcess {
        StringBuilder errors = null;

        for (TokenProcessor processor : processors) {
            try {
                return processor.extract(token);
            } catch (UnableToProcess e) {
                errors = append(errors, e.getMessage());
            }
        }

        String message = errors == null ? "No TokenProcessors configured" : errors.toString();
        throw new UnableToProcess(message);
    }

    private static StringBuilder append(StringBuilder sb, String message) {
        if (sb == null) {
            sb = new StringBuilder();
        } else {
            sb.append(", ");
        }
        return sb.append(message);
    }
}