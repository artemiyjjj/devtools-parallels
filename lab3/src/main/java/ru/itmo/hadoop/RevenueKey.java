package ru.itmo.hadoop;

import org.apache.hadoop.io.*;

import java.io.*;

public class RevenueKey implements WritableComparable<RevenueKey> {
    private DoubleWritable revenue = new DoubleWritable();
    private Text           category = new Text();

    public RevenueKey() {}

    public RevenueKey(double revenue, String category) {
        set(revenue, category);
    }

    public void set(double rev, String cat) {
        this.revenue.set(rev);
        this.category.set(cat);
    }

    public double getRevenue() { return revenue.get(); }
    public String getCategory() { return category.toString(); }

    @Override
    public void write(DataOutput out) throws IOException {
        revenue.write(out);
        category.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue.readFields(in);
        category.readFields(in);
    }

    @Override
    public int compareTo(RevenueKey o) {
        // Минус для нисходящей сортировки
        int cmp = -Double.compare(this.revenue.get(), o.revenue.get());
        if (cmp != 0) return cmp;
        return this.category.compareTo(o.category);
    }

    @Override
    public String toString() {
        return String.format("%s\t%.2f", category.toString(), revenue.get());
    }

    @Override
    public int hashCode() {
        return revenue.hashCode() * 163 + category.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RevenueKey) {
            RevenueKey other = (RevenueKey) o;
            return revenue.equals(other.revenue) && category.equals(other.category);
        }
        return false;
    }
}