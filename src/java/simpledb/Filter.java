package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate preHolder;
    DbIterator[] iterator;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        preHolder=p;
        iterator=new DbIterator[1];
        iterator[0]=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return preHolder;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return iterator[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator[0].open();
        super.open();
    }

    public void close() {
        // some code goes here
        iterator[0].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while(iterator[0].hasNext()){
            Tuple x=iterator[0].next();
            if(preHolder.filter(x)){
                return x;
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return iterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        iterator[0]=children[0];
    }

}
