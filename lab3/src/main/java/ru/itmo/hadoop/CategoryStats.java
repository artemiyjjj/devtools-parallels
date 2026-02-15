package ru.itmo.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class CategoryStats implements Writable {
    private DoubleWritable revenue = new DoubleWritable();
    private LongWritable   quantity = new LongWritable();

    public CategoryStats() {}

    public CategoryStats(double revenue, long quantity) {
        set(revenue, quantity);
    }

    public void set(double revenue, long quantity) {
        this.revenue.set(revenue);
        this.quantity.set(quantity);
    }

    public double getRevenue()   { return revenue.get(); }
    public long   getQuantity()  { return quantity.get(); }

    public void add(double rev, long qty) {
        revenue.set(revenue.get() + rev);
        quantity.set(quantity.get() + qty);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        revenue.write(out);
        quantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue.readFields(in);
        quantity.readFields(in);
    }

    @Override
    public String toString() {
        // Форматируем как "revenue\tquantity"
        return String.format("%.2f\t%d", revenue.get(), quantity.get());
    }
}
