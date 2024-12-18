package com.itmo.reducer;

import com.itmo.model.ValueAsKeyData;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValueAsKeyReducer extends Reducer<DoubleWritable, ValueAsKeyData, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<ValueAsKeyData> values, Context context) throws IOException, InterruptedException {
        for (ValueAsKeyData value : values) {
            context.write(new Text(value.getCategory()),
                    new Text(String.format("%.2f\t%d", -key.get(), value.getQuantity())));
        }
    }
}
