//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package de.tuberlin.dima.minidb.io.cache;

public class TupleKey implements Comparable<TupleKey> {
    private int a;
    private int b;
    private int c;

    public TupleKey(int var1, int var2) {
        this.a = var1;
        this.b = var2;
        this.c = var1 ^ var2;
    }

    public boolean equals(Object var1) {
        return var1 instanceof TupleKey ? this.equals((TupleKey)var1) : false;
    }

    public boolean equals(TupleKey var1) {
        return this.a == var1.a && this.b == var1.b;
    }

    public int getResourceId() {
        return this.a;
    }

    public int getPageNumber() {
        return this.b;
    }

    public int compareTo(TupleKey var1) {
        if (this.a > var1.a) {
            return 1;
        } else if (this.a < var1.a) {
            return -1;
        } else if (this.b > var1.b) {
            return 1;
        } else {
            return this.b < var1.b ? -1 : 0;
        }
    }

    public int hashCode() {
        return this.c;
    }
}
