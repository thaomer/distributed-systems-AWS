package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import java.util.LinkedList;
import java.util.List;

public class Map1Value implements WritableComparable<Map1Value> {

    protected long corpusZeroOccurrences;
    protected long corpusOneOccurrences;


    public Map1Value() {
    }
    public Map1Value(long corpusOneOccurrences, long corpusZeroOccurrences) {
        this.corpusZeroOccurrences = corpusZeroOccurrences;
        this.corpusOneOccurrences = corpusOneOccurrences;
    }


    public long getCorpusZeroCount() {
        return corpusZeroOccurrences;
    }
    public long getCorpusOneCount() {
        return corpusOneOccurrences;
    }

    public void readFields(DataInput in) throws IOException {
        corpusZeroOccurrences = in.readLong();
        corpusOneOccurrences = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(corpusZeroOccurrences);
        out.writeLong(corpusOneOccurrences);
    }

    public String toString() {
        return ("corpus 0 count: " + corpusZeroOccurrences + " ,  corpus 1 count: " + corpusOneOccurrences);
    }
    @Override
    public int compareTo(Map1Value o) {
        return 0;
    }
}
