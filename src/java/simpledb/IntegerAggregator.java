package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Integer, Integer> inthash;
    private HashMap<String, Integer> strhash;
    private HashMap<Integer, Integer> inthashc;
    private HashMap<String, Integer> strhashc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        inthash = new HashMap<Integer, Integer>();
        strhash = new HashMap<String, Integer>();
        inthashc = new HashMap<Integer, Integer>();
        strhashc = new HashMap<String, Integer>();

    }

    public boolean hasGrouping() {
        if (gbfield == Aggregator.NO_GROUPING) {
            return false;
        }
        return true;
    }

    private TupleDesc makeTupleDesc() {
        if (hasGrouping()) {
            Type[] types = new Type[2];
            Type afieldtype = Type.INT_TYPE;
            types[0] = gbfieldtype;         
            types[1] = afieldtype;  
            String[] names = new String[2];
            names[0] = "GROUP BY VAL";
            names[1] = what.toString() + " RESULT";
            return new TupleDesc(types, names);
        } else {
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            return new TupleDesc(types);
        }
    }

    private Field getGBField(Tuple tup) {
        if (!hasGrouping()) {
                return new IntField(0);
        } else if (gbfieldtype == Type.INT_TYPE) {
                return (IntField)(tup.getField(gbfield));
        } else {
                return (StringField)(tup.getField(gbfield));
        }
    }

    private Field getAggField(Tuple tup) {
        return (IntField)(tup.getField(afield));
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //(COUNT, SUM, AVG, MIN, MAX)

        if (what == Op.COUNT) {
            if (gbfieldtype == Type.INT_TYPE) {
                int key = (((IntField)(getGBField(tup))).getValue());
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + 1);
                } else {
                    inthash.put(key, 1);
                }
            }

            if (gbfieldtype == Type.STRING_TYPE) {
                String key = (((StringField)(getGBField(tup))).getValue());
                if (strhash.containsKey(key)) {
                    strhash.put(key, strhash.get(key) + 1);
                } else {
                    strhash.put(key, 1);
                }
            }

            if(gbfieldtype == null) {
                int key = Integer.MIN_VALUE;
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + 1);
                } else {
                    inthash.put(key, 1);
                }
            }
        }

        if (what == Op.SUM) {
            if (gbfieldtype == Type.INT_TYPE) {
                int key = (((IntField)(getGBField(tup))).getValue());
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + ((IntField)(getAggField(tup))).getValue());
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == Type.STRING_TYPE) {
                String key = ((StringField)(getGBField(tup))).getValue();
                if (strhash.containsKey(key)) {
                    strhash.put(key, strhash.get(key) + ((IntField)(getAggField(tup))).getValue());
                } else {
                    strhash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == null) {
                int key = Integer.MIN_VALUE;
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + ((IntField)(getAggField(tup))).getValue());
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
        }

        if (what == Op.AVG) {
            if (gbfieldtype == Type.INT_TYPE) {
                int key = ((IntField)(getGBField(tup))).getValue();
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + ((IntField)(getAggField(tup))).getValue());
                    inthashc.put(key, inthashc.get(key) + 1);
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                    inthashc.put(key, 1);
                }
            }
            if (gbfieldtype == Type.STRING_TYPE) {
                String key = ((StringField)(getGBField(tup))).getValue();
                if (strhash.containsKey(key)) {
                    strhash.put(key, strhash.get(key) + ((IntField)(getAggField(tup))).getValue());
                    strhashc.put(key, strhashc.get(key) + 1);
                } else {
                    strhash.put(key, ((IntField)(getAggField(tup))).getValue());
                    strhashc.put(key, 1);
                }
            }
            if (gbfieldtype == null) {
                int key = Integer.MIN_VALUE;
                if (inthash.containsKey(key)) {
                    inthash.put(key, inthash.get(key) + ((IntField)(getAggField(tup))).getValue());
                    inthashc.put(key, inthashc.get(key) + 1);
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                    inthashc.put(key, 1);
                }
            }
        }

        if (what == Op.MIN) {
            if (gbfieldtype == Type.INT_TYPE) {
                int key = ((IntField)(getGBField(tup))).getValue();
                if (inthash.containsKey(key)) {
                    inthash.put(key, Math.min(inthash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == Type.STRING_TYPE) {
                String key = ((StringField)(getGBField(tup))).getValue();
                if (strhash.containsKey(key)) {
                    strhash.put(key, Math.min(strhash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    strhash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == null) {
                int key = Integer.MIN_VALUE;
                if (inthash.containsKey(key)) {
                    inthash.put(key, Math.min(inthash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
        }

        if (what == Op.MAX) {
            if (gbfieldtype == Type.INT_TYPE) {
                int key = ((IntField)(getGBField(tup))).getValue();
                if (inthash.containsKey(key)) {
                    inthash.put(key, Math.max(inthash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == Type.STRING_TYPE) {
                String key = ((StringField)(getGBField(tup))).getValue();
                if (strhash.containsKey(key)) {
                    strhash.put(key, Math.max(strhash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    strhash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
            if (gbfieldtype == null) {
                int key = Integer.MIN_VALUE;
                if (inthash.containsKey(key)) {
                    inthash.put(key, Math.max(inthash.get(key), ((IntField)(getAggField(tup))).getValue()));
                } else {
                    inthash.put(key, ((IntField)(getAggField(tup))).getValue());
                }
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new
        //UnsupportedOperationException("please implement me for proj2");
        TupleDesc td = makeTupleDesc();
        ArrayList<Tuple> res = new ArrayList<Tuple>();

        for (Integer key : inthash.keySet()) {
            Tuple t = new Tuple(td);
            int val = inthash.get(key);
            if (what == Op.AVG) {
                val = (val / (inthashc.get(key)));
            }
            IntField gb = new IntField(key);
            IntField a = new IntField(val);

            if (hasGrouping()) {
                t.setField(0, gb);
                t.setField(1, a);
            } else {
                t.setField(0, a);
            }
            res.add(t);
        }
        for (String key : strhash.keySet()) {
            Tuple t = new Tuple(td);
            int val = strhash.get(key);
            if (what == Op.AVG) {
                val = (val / (strhashc.get(key)));
            }
            StringField gb = new StringField(key, Type.STRING_LEN);
            IntField a = new IntField(val);
            if (hasGrouping()) {
                t.setField(0, gb);
                t.setField(1, a);
            } else {
                t.setField(0, a);
            }
            res.add(t);
        }
        return new TupleIterator(td, res);
    }

}
