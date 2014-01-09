package simpledb;
import java.io.IOException;
/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    TransactionId tid;
    DbIterator[] iter;
    int fetchnext;
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        iter=new DbIterator[1];
        tid=t;
        iter[0]=child;
        
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] types=new Type[1];
        types[0]=Type.INT_TYPE;
        String[] names=new String[1];
        names[0]="deletedrecords";
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int counter=0;
        if(fetchnext==0){
            while(iter[0].hasNext()){
                counter++;
                BufferPool temp=Database.getBufferPool();
                Tuple t=iter[0].next();
                temp.deleteTuple(tid,t);
            }
            Type[] types=new Type[1];
            types[0]=Type.INT_TYPE;
            String[] names=new String[1];
            names[0]="deletedrecords";
            TupleDesc scheme=new TupleDesc(types,names);
            Tuple t=new Tuple(scheme);
            IntField deletes=new IntField(counter);
            t.setField(0,deletes);
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
