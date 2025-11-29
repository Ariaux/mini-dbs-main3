package de.tuberlin.dima.minidb.io.manager;

import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;

import java.io.IOException;

/**
 * Abstract class for resource managers.
 */
public abstract class ResourceManager
{
    public abstract PageSize getPageSize();

    public abstract void truncate() throws IOException;

    public abstract void closeResource() throws IOException;

    public abstract CacheableData readPageFromResource(byte[] buffer, int pageNumber)
            throws IOException;

    public abstract CacheableData[] readPagesFromResource(byte[][] buffers, int firstPageNumber)
            throws IOException;

    public abstract void writePageToResource(byte[] buffer, CacheableData wrapper)
            throws IOException;

    public abstract void writePagesToResource(byte[][] buffers, CacheableData[] wrappers)
            throws IOException;

    public abstract CacheableData reserveNewPage(byte[] ioBuffer)
            throws IOException, PageFormatException;

    public abstract CacheableData reserveNewPage(byte[] ioBuffer, Enum<?> type)
            throws IOException, PageFormatException;

    // 必须包含这个抽象方法
    public abstract CacheableData readPage(byte[] buffer, int pageNumber)
            throws IOException, PageFormatException;
}