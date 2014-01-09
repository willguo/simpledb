package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    TransactionId tid;
    DbIterator[] iter;
    int id;
    int fetchnext;
    TupleDesc actual;
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        tid=t;
        iter=new DbIterator[1];
        iter[0]=child;
        id=tableid;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
            Type[] types=new Type[1];
            types[0]=Type.INT_TYPE;
            String[] names=new String[1];
            names[0]="records";
            TupleDesc scheme=new TupleDesc(types,names);
            return scheme;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        iter[0].open();
        super.open();
        fetchnext=0;
    }

    public void close() {
        // some code goes here
        iter[0].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iter[0].rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int counter=0;
        if(fetchnext==0){
            while(iter[0].hasNext()){
                counter++;
                BufferPool temp=Database.getBufferPool();
                Tuple t=iter[0].next();
                try{
                    temp.insertTuple(tid,id,t);
                } catch (IOException i){
                    throw new DbException("Failed to insert");
                }


            }
            Type[] types=new Type[1];
            types[0]=Type.INT_TYPE;
            String[] names=new String[1];
            names[0]="records";
            TupleDesc scheme=new TupleDesc(types,names);
            Tuple t=new Tuple(scheme);
            IntField inserts=new IntField(counter);
            t.setField(0,inserts);
            fetchnext++;
            return t;
        }
        return null;
            
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return iter;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        iter[0]=children[0];
    }
}
