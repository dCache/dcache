/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera.store;

/*
 *	@Immutable
 */
public final class AccessLatency {

    private final String _name;
    private final int _id;
    public static final AccessLatency ONLINE = new AccessLatency("ONLINE", 1);
    public static final AccessLatency NEARLINE = new AccessLatency("NEARLINE", 0);

    private AccessLatency(String name, int id) {
        _name = name;
        _id = id;
    }

    public static AccessLatency[] getAllLatencies() {
        return new AccessLatency[]{
                    ONLINE,
                    NEARLINE};
    }

    @Override
    public String toString() {
        return _name;
    }

    public int getId() {
        return _id;
    }

    public static AccessLatency valueOf(String state) throws IllegalArgumentException {
        if (state == null || state.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        if (ONLINE._name.equalsIgnoreCase(state)) {
            return ONLINE;
        }

        if (NEARLINE._name.equalsIgnoreCase(state)) {
            return NEARLINE;
        }

        try {
            int id = Integer.parseInt(state);
            return valueOf(id);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown AccessLatency");
        }
    }

    public static AccessLatency valueOf(int id) throws IllegalArgumentException {

        if (ONLINE._id == id) {
            return ONLINE;
        }

        if (NEARLINE._id == id) {
            return NEARLINE;
        }

        throw new IllegalArgumentException("Unknown State Id");
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof AccessLatency) && (((AccessLatency) obj).getId() == this.getId());
    }

    @Override
    public int hashCode() {
        return _id;
    }
}
