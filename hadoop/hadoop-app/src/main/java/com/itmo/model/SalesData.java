package com.itmo.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesData implements Writable {
    private double revenue;
    private int quantity;

    public SalesData() {
    }

    public SalesData(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    public void set(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }

    @Override
    public String toString() {
        return revenue + "\t" + quantity;
    }
}

