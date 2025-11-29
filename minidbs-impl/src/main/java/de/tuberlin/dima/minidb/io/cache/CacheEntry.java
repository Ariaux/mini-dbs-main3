//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.cache.CacheableData;

public class CacheEntry {
    private CacheEntry a;
    private CacheEntry b;
    private ARCCacheEntryList c;
    private boolean d;
    private byte[] e;
    private CacheableData f;
    private int g;
    private int h;
    private int i;
    private boolean j;

    public CacheEntry(byte[] var1, CacheableData var2, int var3, int var4, ARCCacheEntryList var5) {
        if (var1 == null) {
            throw new NullPointerException("Binary page buffer must not be null");
        } else {
            this.e = var1;
            this.f = var2;
            this.g = var3;
            this.h = var4;
            this.d = false;
            this.j = true;
            this.c = var5;
            this.i = 0;
        }
    }

    public CacheEntry(byte[] var1, ARCCacheEntryList var2) {
        if (var1 == null) {
            throw new NullPointerException("Binary page buffer must not be null");
        } else {
            this.e = var1;
            this.c = var2;
            this.f = null;
            this.g = -1;
            this.h = -1;
            this.d = false;
            this.j = false;
            this.i = 0;
        }
    }

    public void markInvalid() {
        this.j = false;
    }

    public byte[] getBinaryPage() {
        return this.e;
    }

    public CacheableData getWrappingPage() {
        return this.f;
    }

    public ARCCacheEntryList getList() {
        return this.c;
    }

    public int getResourceID() {
        return this.g;
    }

    public int getPageNumber() {
        return this.h;
    }

    public boolean isValid() {
        return this.j;
    }

    final CacheEntry a() {
        return this.a;
    }

    final CacheEntry b() {
        return this.b;
    }

    final boolean c() {
        return this.i > 0;
    }

    final void a(CacheEntry var1) {
        this.a = var1;
    }

    final void b(CacheEntry var1) {
        this.b = var1;
    }

    final void d() {
        ++this.i;
    }

    final void e() {
        if (this.c()) {
            --this.i;
        }

    }

    final void a(int var1) {
        this.i = 0;
    }

    public void setList(ARCCacheEntryList var1) {
        this.c = var1;
    }

    public boolean wasHit() {
        return this.d;
    }

    public void hit() {
        this.d = true;
    }
}
