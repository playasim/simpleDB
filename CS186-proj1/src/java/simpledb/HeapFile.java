package simpledb;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.numPage = (int) (file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    //id就是tableid
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * 根据PageId从磁盘读取一个页，注意此方法只应该在BufferPool类被直接调用
     * 在其他需要page的地方需要通过BufferPool访问。这样才能实现缓存功能
     *
     * @param pid
     * @return 读取得到的Page
     *
     */
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        //数据会被写入到data中
        byte[] data= new byte[BufferPool.PAGE_SIZE];
        try {
            //RandomAccessFile可以让我们能够在file中访问任意位置
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            //这个Page正确的偏移量
            long offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
            //将file pointer移到这里
            raf.seek(offset);
            //读取数据，将数据存入data中
            raf.read(data, 0, BufferPool.PAGE_SIZE);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid)  {
        // some code goes here
        return new heapFileIterator(tid);
    }

    private class heapFileIterator implements DbFileIterator {

        private int pos;
        private TransactionId tid;
        BufferPool bufferPool = Database.getBufferPool();
        Iterator<Tuple> tuplesInPage;

        private Iterator<Tuple> getTuplesInPage(PageId pageId) throws TransactionAbortedException, DbException {
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_ONLY);
            return page.iterator();
        }

        public heapFileIterator(TransactionId tid) {
            this.tid = tid;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {
            pos = 0;
            HeapPageId pid = new HeapPageId(getId(), pos);
            tuplesInPage = getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException {
            //如果迭代器为空，没有next
            if (tuplesInPage == null)
                return false;

            if (tuplesInPage.hasNext())
                return true;
            //此页遍历完后，检查是否还有下一页
            if (pos < numPages() - 1) {
                pos ++;
                //getId指tableId, pos指page number
                PageId pageId = new HeapPageId(getId(), pos);
                //产生新的迭代器
                tuplesInPage = getTuplesInPage(pageId);
                return tuplesInPage.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, DbException {
            if (!hasNext()) {
                throw new NoSuchElementException("not opened or no tuple remained");
            }
            return tuplesInPage.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        //close和open的区别是：有没有初始化tuplesInPage
        @Override
        public void close() {
            pos = 0;
            tuplesInPage = null;
        }
    }

}

