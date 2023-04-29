package org.example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Map3Key implements WritableComparable<Map3Key> {

    protected String threeGram;
    protected Double probability;

    public Map3Key() {

    }

    public Map3Key(String threeGram, Double probability) {
        this.threeGram = threeGram;
        this.probability = probability;
    }

    public String getThreeGram() {
        return threeGram;
    }

    public Double getProbability() {
        return probability;
    }

    @Override
    public int compareTo(Map3Key other) {
        String [] thisThreeGramAsArray = this.getThreeGram().split("\\s+");
        String [] otherThreeGramAsArray = other.getThreeGram().split("\\s+");

        int firstWordCompare = thisThreeGramAsArray[0].compareTo(otherThreeGramAsArray[0]);
        if (firstWordCompare == 0) {
            int secondWordCompare =  thisThreeGramAsArray[1].compareTo(otherThreeGramAsArray[1]);

            if (secondWordCompare == 0) {
                //compare with other first to revers from ascending to descending
                return (other.getProbability()).compareTo(this.getProbability());
            }
            //if first word is the same but second not the same we just order it by second word ascending
            return secondWordCompare;
        }
        //if first word is not the same we sort by first word ascending order
        return firstWordCompare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.threeGram);
        dataOutput.writeDouble(this.probability);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.threeGram = dataInput.readUTF();
        this.probability = dataInput.readDouble();
    }

    public String toString() {
        return ("threeGram: " + threeGram);
    }
}
