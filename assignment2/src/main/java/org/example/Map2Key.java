package org.example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Map2Key implements WritableComparable<Map2Key> {

    protected long r_value;
    protected boolean isThreeGram;

    public Map2Key() {

    }

    public Map2Key(long r_value, boolean isThreeGram) {
        //the type is index from 0-4 that represent 0=n_r^0, 1=n_r^1, 2=t_r^01, 3=t_r^10, 4=r
        this.r_value = r_value;
        this.isThreeGram = isThreeGram;
    }

    public long getR_value() {
        return r_value;
    }

    public boolean getIsThreeGram() {
        return isThreeGram;
    }

    @Override
    public int compareTo(Map2Key other) {
        //order the count by asencding order
        int countMatch = Long.compare(this.r_value, other.r_value);
        //if count is math order it by the type index
        if (countMatch == 0) {
            return Boolean.compare(this.isThreeGram, other.isThreeGram);
        }
        return countMatch;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.r_value);
        dataOutput.writeBoolean(this.isThreeGram);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.r_value = dataInput.readLong();
        this.isThreeGram = dataInput.readBoolean();
    }
}
