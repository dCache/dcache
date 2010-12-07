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
public final class RetentionPolicy {

    private final String _name;
    private final int _id;
    public static final RetentionPolicy REPLICA = new RetentionPolicy("REPLICA", 2);
    public static final RetentionPolicy OUTPUT = new RetentionPolicy("OUTPUT", 1);
    public static final RetentionPolicy CUSTODIAL = new RetentionPolicy("CUSTODIAL", 0);

    private RetentionPolicy(String name, int id) {
        _name = name;
        _id = id;
    }

    public static RetentionPolicy[] getAllPoliciess() {
        return new RetentionPolicy[]{
                    REPLICA,
                    OUTPUT,
                    CUSTODIAL};
    }

    @Override
    public String toString() {
        return _name;
    }

    public int getId() {
        return _id;
    }

    public static RetentionPolicy valueOf(String state) throws IllegalArgumentException {
        if (state == null || state.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        if (REPLICA._name.equalsIgnoreCase(state)) {
            return REPLICA;
        }

        if (OUTPUT._name.equalsIgnoreCase(state)) {
            return OUTPUT;
        }

        if (CUSTODIAL._name.equalsIgnoreCase(state)) {
            return CUSTODIAL;
        }
        try {
            int id = Integer.parseInt(state);
            return valueOf(id);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown State");
        }
    }

    public static RetentionPolicy valueOf(int id) throws IllegalArgumentException {

        if (REPLICA._id == id) {
            return REPLICA;
        }

        if (OUTPUT._id == id) {
            return OUTPUT;
        }

        if (CUSTODIAL._id == id) {
            return CUSTODIAL;
        }

        throw new IllegalArgumentException("Unknown State Id");
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof RetentionPolicy) && (((RetentionPolicy) obj).getId() == this.getId());
    }

    @Override
    public int hashCode() {
        return _id;
    }
}