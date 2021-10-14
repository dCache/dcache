/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.events.spi;

import static java.util.Objects.requireNonNull;

import javax.validation.constraints.NotNull;

/**
 * This class describes the result of a selection request.
 */
public class SelectionResult {

    private final SelectedEventStream ses;
    private final SelectionStatus status;
    private final String message;

    public static SelectionResult badSelector(@NotNull String template, Object... args) {
        return new SelectionResult(null, SelectionStatus.BAD_SELECTOR,
              String.format(template, args));
    }

    public static SelectionResult created(@NotNull SelectedEventStream ses) {
        return new SelectionResult(ses, SelectionStatus.CREATED, null);
    }

    public static SelectionResult merged(@NotNull SelectedEventStream ses) {
        return new SelectionResult(ses, SelectionStatus.MERGED, null);
    }

    public static SelectionResult permissionDenied(@NotNull String template, Object... args) {
        return new SelectionResult(null, SelectionStatus.PERMISSION_DENIED,
              String.format(template, args));
    }

    public static SelectionResult resourceNotFound(@NotNull String template, Object... args) {
        return new SelectionResult(null, SelectionStatus.RESOURCE_NOT_FOUND,
              String.format(template, args));
    }

    public static SelectionResult conditionFailed(@NotNull String template, Object... args) {
        return new SelectionResult(null, SelectionStatus.CONDITION_FAILED,
              String.format(template, args));
    }

    public static SelectionResult internalError(@NotNull String template, Object... args) {
        return new SelectionResult(null, SelectionStatus.INTERNAL_ERROR,
              String.format(template, args));
    }

    private SelectionResult(SelectedEventStream ses, SelectionStatus status,
          String message) {
        this.ses = ses;
        this.status = requireNonNull(status);
        this.message = message;
    }

    public SelectedEventStream getSelectedEventStream() {
        return ses;
    }

    @NotNull
    public SelectionStatus getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
