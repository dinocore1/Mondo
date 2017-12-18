package com.devsmart.mondo;


import static com.google.common.base.Preconditions.checkNotNull;

public class Name {


    /** The empty name. */
    public static final Name EMPTY = new Name("", "");

    /** The name to use for a link from a directory to itself. */
    public static final Name SELF = new Name(".", ".");

    /** The name to use for a link from a directory to its parent directory. */
    public static final Name PARENT = new Name("..", "..");

    public static Name create(String name) {
        switch(name) {
            case "":
                return EMPTY;
            case ".":
                return SELF;

            case "..":
                return PARENT;

            default:
                return create(name, name);
        }
    }

    public static Name create(String display, String canonical) {
        return new Name(display, canonical);
    }

    public final String display;
    public final String canonical;

    private Name(String display, String canonical) {
        this.display = checkNotNull(display);
        this.canonical = checkNotNull(canonical);
    }

    @Override
    public boolean equals( Object obj) {
        if (obj instanceof Name) {
            Name other = (Name) obj;
            return canonical.equals(other.canonical);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return canonical.hashCode();
    }

    @Override
    public String toString() {
        return display;
    }
}
