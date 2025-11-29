//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class AdaptiveReplacementCache implements PageCache {
    private ARCCacheEntryList a;
    private ARCCacheEntryList b;
    private ARCCacheEntryList c;
    private ARCCacheEntryList d;
    private int e;
    private int f;
    private int g;
    private HashMap<TupleKey, CacheEntry> h;
    private HashMap<TupleKey, CacheEntry> i;

    public AdaptiveReplacementCache(PageSize var1, int var2) {
        this.g = var1.getNumberOfBytes();
        this.f = var2;
        this.h = new HashMap();
        this.i = new HashMap();
        this.e = 0;
        this.a = new ARCCacheEntryList();
        this.b = new ARCCacheEntryList();
        this.c = new ARCCacheEntryList();
        this.d = new ARCCacheEntryList();

        for(int var3 = 0; var3 < var2; ++var3) {
            this.a.addFirst(new CacheEntry(new byte[this.g], (ARCCacheEntryList)null));
        }

    }

    public CacheableData getPage(int var1, int var2) {
        return this.a(var1, var2, false);
    }

    public CacheableData getPageAndPin(int var1, int var2) {
        return this.a(var1, var2, true);
    }

    public EvictedCacheEntry addPage(CacheableData var1, int var2) throws CachePinnedException, DuplicateCacheEntryException {
        return this.a(var1, var2, false);
    }

    public EvictedCacheEntry addPageAndPin(CacheableData var1, int var2) throws CachePinnedException, DuplicateCacheEntryException {
        return this.a(var1, var2, true);
    }

    public void unpinPage(int var1, int var2) {
        TupleKey var3 = new TupleKey(var1, var2);
        CacheEntry var4;
        if ((var4 = (CacheEntry)this.h.get(var3)) != null) {
            var4.e();
        }

    }

    public CacheableData[] getAllPagesForResource(int var1) {
        HashSet var2 = new HashSet();
        Iterator var3 = this.h.keySet().iterator();

        while(var3.hasNext()) {
            TupleKey var4;
            if ((var4 = (TupleKey)var3.next()).getResourceId() == var1) {
                CacheEntry var5 = (CacheEntry)this.h.get(var4);
                var2.add(var5.getWrappingPage());
                this.a(var5);
            }
        }

        return (CacheableData[])var2.toArray(new CacheableData[var2.size()]);
    }

    public void expelAllPagesForResource(int var1) {
        HashSet var2 = new HashSet();
        Iterator var3 = this.h.keySet().iterator();

        while(var3.hasNext()) {
            TupleKey var4;
            if ((var4 = (TupleKey)var3.next()).getResourceId() == var1) {
                CacheEntry var5 = (CacheEntry)this.h.get(var4);
                var2.add(var4);
                var5.a(0);
                var5.getList().remove(var5);
                this.a.addLast(var5);
            }
        }

        for(Object var7 : var2) {
            this.h.remove(var7);
        }

    }

    public int getCapacity() {
        return this.f;
    }

    public void unpinAllPages() {
        Iterator var1 = this.h.values().iterator();

        while(var1.hasNext()) {
            ((CacheEntry)var1.next()).a(0);
        }

    }

    private void a(CacheEntry var1) {
        var1.getList().remove(var1);
        this.c.addFirst(var1);
    }

    private void b(CacheEntry var1) {
        var1.getList().remove(var1);
        this.a.addFirst(var1);
    }

    private CacheableData a(int var1, int var2, boolean var3) {
        TupleKey var4 = new TupleKey(var1, var2);
        CacheEntry var5;
        if ((var5 = (CacheEntry)this.h.get(var4)) != null) {
            if (var5.wasHit()) {
                this.a(var5);
            } else {
                this.b(var5);
                var5.hit();
            }

            if (var3) {
                var5.d();
            }

            return var5.getWrappingPage();
        } else {
            return null;
        }
    }

    private EvictedCacheEntry a(CacheableData var1, int var2, boolean var3) throws DuplicateCacheEntryException, CachePinnedException {
        TupleKey var4 = new TupleKey(var2, var1.getPageNumber());
        if (this.h.get(var4) != null) {
            throw new DuplicateCacheEntryException(var2, var1.getPageNumber());
        } else {
            CacheEntry var8 = new CacheEntry(var1.getBuffer(), var1, var2, var1.getPageNumber(), (ARCCacheEntryList)null);
            if (var3) {
                var8.d();
                var8.hit();
            }

            CacheEntry var5;
            CacheEntry var9;
            if ((var9 = (CacheEntry)this.i.get(var4)) != null) {
                if (var9.getList() == this.b) {
                    this.a();
                    var5 = this.c(var9);
                    this.b.remove(var9);
                    this.i.remove(var4);
                    this.c.addFirst(var8);
                    var8.hit();
                    this.h.put(var4, var8);
                } else {
                    this.b();
                    var5 = this.c(var9);
                    this.d.remove(var9);
                    this.i.remove(var4);
                    this.c.addFirst(var8);
                    var8.hit();
                    this.h.put(var4, var8);
                }
            } else {
                label38: {
                    if (this.a.size() + this.b.size() == this.f) {
                        if (this.a.size() >= this.f) {
                            if ((var5 = this.a.removeLastUnpinned()) == null && (var5 = this.c.removeLastUnpinned()) == null) {
                                throw new CachePinnedException();
                            }

                            this.h.remove(new TupleKey(var5.getResourceID(), var5.getPageNumber()));
                            break label38;
                        }

                        var5 = this.b.removeLast();
                        this.i.remove(new TupleKey(var5.getResourceID(), var5.getPageNumber()));
                    } else if (this.a.size() + this.c.size() + this.b.size() + this.d.size() == 2 * this.f) {
                        var5 = this.d.removeLast();
                        this.i.remove(new TupleKey(var5.getResourceID(), var5.getPageNumber()));
                    }

                    var5 = this.c(var8);
                }

                this.a.addFirst(var8);
                this.h.put(var4, var8);
            }

            return var5.isValid() ? new EvictedCacheEntry(var5.getBinaryPage(), var5.getWrappingPage(), var5.getResourceID()) : new EvictedCacheEntry(var5.getBinaryPage());
        }
    }

    private CacheEntry c(CacheEntry var1) throws CachePinnedException {
        if (this.a.isEmpty() || this.a.size() <= this.e && (this.a.size() != this.e || var1.getList() != this.d)) {
            if ((var1 = this.c.removeLastUnpinned()) != null) {
                TupleKey var6 = new TupleKey(var1.getResourceID(), var1.getPageNumber());
                this.h.remove(var6);
                this.d.addFirst(var1);
                this.i.put(var6, var1);
            } else {
                if ((var1 = this.a.removeLastUnpinned()) == null) {
                    throw new CachePinnedException();
                }

                TupleKey var7 = new TupleKey(var1.getResourceID(), var1.getPageNumber());
                this.h.remove(var7);
                this.b.addFirst(var1);
                this.i.put(var7, var1);
            }

            return var1;
        } else {
            if ((var1 = this.a.removeLastUnpinned()) != null) {
                TupleKey var2 = new TupleKey(var1.getResourceID(), var1.getPageNumber());
                this.b.addFirst(var1);
                this.h.remove(var2);
                this.i.put(var2, var1);
            } else {
                if ((var1 = this.c.removeLastUnpinned()) == null) {
                    throw new CachePinnedException();
                }

                TupleKey var5 = new TupleKey(var1.getResourceID(), var1.getPageNumber());
                this.d.addFirst(var1);
                this.h.remove(var5);
                this.i.put(var5, var1);
            }

            return var1;
        }
    }

    private void a() {
        int var1 = 1;
        if (this.b.size() < this.d.size()) {
            var1 = this.d.size() / this.b.size();
        }

        this.e = Math.min(this.e + var1, this.f);
    }

    private void b() {
        int var1 = 1;
        if (this.d.size() < this.b.size()) {
            var1 = this.b.size() / this.d.size();
        }

        this.e = Math.max(this.e - var1, 0);
    }
}
