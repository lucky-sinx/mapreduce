package com.wy.mapreduce.bigdatahomework;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateKey implements WritableComparable<DateKey> {
    private String date;

    @Override
    public int compareTo(DateKey o) {
        return compareDate(stringToDate(this.date), stringToDate(o.date));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
    }

    public static Date stringToDate(String dateString) {
        return stringToDate(dateString, "yyyy-MM-dd");
    }

    public static Date stringToDate(String dateText, String format) {

        DateFormat df = null;
        try {
            if (format == null) {
                df = new SimpleDateFormat();
            } else {
                df = new SimpleDateFormat(format);
            }
            df.setLenient(false);

            return df.parse(dateText);
        } catch (ParseException e) {
            return null;
        }
    }

    public static int compareDate(Date firstTime, Date secondTime) {

        long lFirstTime = firstTime.getTime();
        long lsecondTime = secondTime.getTime();

        if (lFirstTime < lsecondTime) {
            return -1;
        } else if (lFirstTime > lsecondTime) {
            return 1;
        } else {
            return 0;
        }
    }
}
