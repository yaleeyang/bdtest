package com.tuqu.sunny.mr;

import com.tuqu.sunny.utils.CommandUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by sunny on 17/7/31.
 * clean repeat data
 */
public class ClearTrajectoryData extends Configured implements Tool {


    public static class GeoMap extends Mapper<LongWritable,Text,Text,NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if(value == null || StringUtils.isBlank(value.toString())){
                return;
            }

            String line = value.toString();
            //对时间（datetime）重复的数据进行清洗
            String fristData = CommandUtils.removeRepeatDate(line);
            //把前后记录经纬度前三位重复的清洗掉
            String endData = CommandUtils.removeSameData(fristData);
            if(endData == null)
                return;
            context.write(new Text(endData), NullWritable.get());
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        job.setMapperClass(GeoMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //不需要reduce阶段
        job.setNumReduceTasks(0);

        //int path
        Path inpath = new Path(args[0]);
        FileInputFormat.setInputPaths(job,inpath);

        //out path
        Path outpath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        // 路径存在删除
        if (fs.exists(outpath)){
            fs.delete(outpath,true);
        }
        FileOutputFormat.setOutputPath(job,outpath);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int code =ToolRunner.run(conf,new ClearTrajectoryData(),args);
            System.exit(code);
        } catch (Exception e) {
            e.printStackTrace();
        }

//  本地输入源： yarn jar /opt/sunny/Jars/geo.jar com.tuqu.sunny.mr.ClearTrajectoryData file:///opt/sunny/T2016-12-25.csv  /tmp/output1
    }
}
