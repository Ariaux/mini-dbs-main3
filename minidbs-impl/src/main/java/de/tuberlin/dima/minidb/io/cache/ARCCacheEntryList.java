//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package de.tuberlin.dima.minidb.io.cache;

public class ARCCacheEntryList {
    private int a = 0;
    private CacheEntry b = null;
    private CacheEntry c = null;

    public void addFirst(CacheEntry var1) {
        if (var1 != null) {
            if (this.b == null) {
                var1.b((CacheEntry)null);
                this.c = var1;
            } else {
                this.b.a(var1);
                var1.b(this.b);
            }

            var1.a((CacheEntry)null);
            var1.setList(this);
            this.b = var1;
            ++this.a;
        }

    }

    public void addLast(CacheEntry var1) {
        if (var1 != null) {
            if (this.c == null) {
                var1.a((CacheEntry)null);
                this.b = var1;
            } else {
                this.c.b(var1);
                var1.a(this.c);
            }

            var1.b((CacheEntry)null);
            var1.setList(this);
            this.c = var1;
            ++this.a;
        }

    }

    public void clear() {
        this.b = null;
        this.c = null;
        this.a = 0;
    }

    public boolean isEmpty() {
        return this.a == 0;
    }

    public void remove(CacheEntry var1) {
        if (var1 != null) {
            if (this.c == var1) {
                this.removeLast();
                return;
            }

            if (this.b == var1) {
                this.removeFirst();
                return;
            }

            CacheEntry var2 = var1.a();
            CacheEntry var3 = var1.b();
            var2.b(var3);
            var3.a(var2);
            var1.b((CacheEntry)null);
            var1.a((CacheEntry)null);
            var1.setList((ARCCacheEntryList)null);
            --this.a;
        }

    }

    public CacheEntry removeLast() {
        if (this.c != null) {
            CacheEntry var1 = this.c;
            if (this.c != this.b) {
                CacheEntry var2;
                (var2 = this.c.a()).b((CacheEntry)null);
                this.c = var2;
                --this.a;
            } else {
                this.clear();
            }

            var1.setList((ARCCacheEntryList)null);
            var1.a((CacheEntry)null);
            return var1;
        } else {
            return null;
        }
    }

    public CacheEntry removeFirst() {
        if (this.b != null) {
            CacheEntry var1 = this.b;
            if (this.b != this.c) {
                CacheEntry var2;
                (var2 = this.b.b()).a((CacheEntry)null);
                this.b = var2;
                --this.a;
            } else {
                this.clear();
            }

            var1.setList((ARCCacheEntryList)null);
            var1.b((CacheEntry)null);
            return this.b;
        } else {
            return null;
        }
    }

    public CacheEntry removeLastUnpinned() {
        if (this.c != null) {
            CacheEntry var1;
            for(var1 = this.c; var1 != null && var1.c(); var1 = var1.a()) {
            }

            if (var1 != null) {
                this.remove(var1);
                return var1;
            }
        }

        return null;
    }

    public int size() {
        return this.a;
    }
}
