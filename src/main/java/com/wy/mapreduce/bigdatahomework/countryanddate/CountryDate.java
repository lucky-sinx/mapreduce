package com.wy.mapreduce.bigdatahomework.countryanddate;

import com.wy.mapreduce.bigdatahomework.CountryTable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryDate implements WritableComparable<CountryDate> {
    private String country;
    private String date;

    public String getCountry() {
        return country;
    }

    public String getDate() {
        return date;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public int compareTo(CountryDate o) {
        int compareCountry = this.country.compareTo(o.getCountry());
        if (compareCountry == 0) {
            int compareDate = this.date.compareTo(o.getDate());
//            int compareDate = this.date.substring(0, 9).compareTo(o.getDate().substring(0, 9));
            return compareDate;
        } else {
            return compareCountry;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.country);
        out.writeUTF(this.date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.country = in.readUTF();
        this.date = in.readUTF();
    }
}
