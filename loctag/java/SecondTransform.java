package com.tuqu.sunny.hive;

import com.tuqu.sunny.utils.CommandUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Second transform hour
 * Created by sunny on 17/8/2.
 */
public class SecondTransform extends UDF {

    /**
     * sencond format hour
     * @param second
     * @return formated time
     */
    public FloatWritable evaluate(final LongWritable second){
        //防止为负数的情况
        long sd = Math.abs(second.get());
        //返回小时
        float format_time = CommandUtils.secondTransform(sd);

        return new FloatWritable(format_time);
    }
}
