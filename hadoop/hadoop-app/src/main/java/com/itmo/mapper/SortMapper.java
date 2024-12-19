package com.itmo.mapper;

import com.itmo.model.SortData;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, SortData> {
    private final DoubleWritable outKey = new DoubleWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length == 3) {
            try {
                String categoryKey = fields[0];
                double val = Double.parseDouble(fields[1]);
                int quantity = Integer.parseInt(fields[2]);
                outKey.set(-1 * val);
                context.write(outKey, new SortData(categoryKey, quantity));
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid record: " + value);
            }
        }
    }
}
