package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {


    /**
     * tuple fields array
     */
    private TDItem[] tdItems;
    /**
     * number of fields in this TD
     */
    private int numFields;

    public TupleDesc() {

    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(TDItem tdItem) {
            if (this.fieldType.equals(tdItem.fieldType))
                return true;
            return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemIterator();
    }

    //field TDItems Iterator
    private class TDItemIterator implements Iterator<TDItem> {

        private int position = 0;
        @Override
        public boolean hasNext() {
            if (tdItems.length > position) return true;
            return false;
        }

        @Override
        public TDItem next() {
            return tdItems[position ++];
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length == 0)
            throw new IllegalArgumentException("typeArray can not have no elements");
        if (typeAr.length != fieldAr.length)
            throw new IllegalArgumentException("fieldArray length should equal to typeArray length");
        numFields = typeAr.length;
        tdItems = new TDItem[numFields];
        for (int index = 0; index < typeAr.length; index ++) {
            tdItems[index] = new TDItem(typeAr[index], fieldAr[index]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length == 0)
            throw new IllegalArgumentException("typeArray can not have no elements");
        tdItems = new TDItem[typeAr.length];
        this.numFields = typeAr.length;
        for (int index = 0; index < typeAr.length; index ++) {
            tdItems[index] = new TDItem(typeAr[index], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tdItems.length; i ++) {
            if (tdItems[i].fieldName == null)
                break;
            if (tdItems[i].fieldName.equals(name))
                return i;
        }

        throw new NoSuchElementException("no such element in Tuple");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     *
     * @description example: tdItems[4]
     *                fields: [String, Int, Int, String]
     *                size: String.getlen() * 2 + Int.getlen() * 2
     */
    public int getSize() {
        // some code goes here
        int res = 0;
        for (int i = 0; i < numFields; i ++) {
            if (tdItems[i].fieldType.equals(Type.INT_TYPE))
                res += Type.INT_TYPE.getLen();
            if (tdItems[i].fieldType.equals(Type.STRING_TYPE))
                res += Type.STRING_TYPE.getLen();
        }
        return res;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc res = new TupleDesc();
        res.numFields = td1.numFields + td2.numFields;
        int pos = 0;
        res.tdItems = new TDItem[res.numFields];
        for (int i = 0; i < td1.numFields; i ++) {
            res.tdItems[i] = new TDItem( td1.tdItems[i].fieldType,  td1.tdItems[i].fieldName);
            pos ++;
        }
        for (int i = 0; i < td2.numFields; i++) {
            res.tdItems[i + pos] = new TDItem( td2.tdItems[i].fieldType,  td2.tdItems[i].fieldName);
        }

        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == this)
            return true;
        if ( !(o instanceof TupleDesc))
            return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        if (tupleDesc.numFields != this.numFields)
            return false;
        for (int i = 0; i < numFields; i ++) {
            if (!tupleDesc.tdItems[i].equals(this.tdItems[i]))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder("");
        for (int i = 0; i < numFields; i ++) {
            TDItem tdItem = tdItems[i];
            stringBuilder.append(tdItem.fieldType).append("(").append(tdItem.fieldName).append("),");
        }
        return stringBuilder.toString();
    }
}
