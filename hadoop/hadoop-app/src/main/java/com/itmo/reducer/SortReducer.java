package com.itmo.reducer;

import com.itmo.model.SortData;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, SortData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<SortData> values, Context context) throws IOException, InterruptedException {
        for (SortData value : values) {
            context.write(new Text(value.getCategory()),
                    new Text(String.format("%.2f\t%d", -key.get(), value.getQuantity())));
        }
    }
}
