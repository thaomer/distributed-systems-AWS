package org.example;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Map2Value implements WritableComparable<Map2Value> {

    protected String threeGram;
    protected long Nr_0;
    protected long Nr_1;
    protected long Tr_01;
    protected long Tr_10;

    public Map2Value() {

    }

    public Map2Value(String threeGram, long Nr_0, long Nr_1, long Tr_01, long Tr_10) {
        //the type is index from 0-4 that represent 0=n_r^0, 1=n_r^1, 2=t_r^01, 3=t_r^10, 4=r
        this.threeGram = threeGram;
        this.Nr_0 = Nr_0;
        this.Nr_1 = Nr_1;
        this.Tr_01 = Tr_01;
        this.Tr_10 =Tr_10;
    }

    public String getThreeGram() {
        return threeGram;
    }

    public long getNr_0() {
        return Nr_0;
    }

    public long getNr_1() {
        return Nr_1;
    }

    public long getTr_01() {
        return Tr_01;
    }

    public long getTr_10() {
        return Tr_10;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.threeGram);
        dataOutput.writeLong(this.Nr_0);
        dataOutput.writeLong(this.Nr_1);
        dataOutput.writeLong(this.Tr_01);
        dataOutput.writeLong(this.Tr_10);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.threeGram = dataInput.readUTF();
        this.Nr_0 = dataInput.readLong();
        this.Nr_1 = dataInput.readLong();
        this.Tr_01 = dataInput.readLong();
        this.Tr_10 = dataInput.readLong();
    }

    @Override
    public int compareTo(Map2Value o) {
        return 0;
    }
}
