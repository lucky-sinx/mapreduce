package com.wy.mapreduce.bigdatahomework.countryanddate;

import com.wy.mapreduce.bigdatahomework.CountryTable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class CountryDateReducer extends Reducer<CountryDate, CountryTable, CountryTable, NullWritable> {
    @Override
    protected void reduce(CountryDate key, Iterable<CountryTable> values, Reducer<CountryDate, CountryTable, CountryTable, NullWritable>.Context context) throws IOException, InterruptedException {
        CountryTable max_popular = new CountryTable();
        max_popular.setPopularityScore(-1);
        for (CountryTable value : values) {
            if (max_popular.getPopularityScore() < value.getPopularityScore() ) {
//            if (value.getQuery().equals("coronavirus")) {
                try {
                    BeanUtils.copyProperties(max_popular, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        if(max_popular.getPopularityScore()!=-1)
        {
            context.write(max_popular, NullWritable.get());
        }
    }
}
