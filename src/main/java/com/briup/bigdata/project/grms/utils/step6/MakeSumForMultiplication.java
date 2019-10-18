package com.briup.bigdata.project.grms.utils.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step1
 * @CreateDate: 2019/10/11 14:35
 * @Description:
 */
public class MakeSumForMultiplication extends Configured implements Tool {
   public static class MakeSumForMultiplicationMapper
            extends Mapper<LongWritable, Text,Text,Text> {
        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] str=v1.toString().split("[\t]");
            this.k2.set(str[0]);
            this.v2.set(str[1]);
            context.write(this.k2,this.v2);
        }
    }
   public static class MakeSumForMultiplicationReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            this.k3.set(k2.toString());
            List<Text> list=new ArrayList<>();
            int count=0;
            v2s.forEach(val->{
                list.add(new Text(val.toString()));
            });

            for (Text text : list) {
                int a=Integer.parseInt(text.toString());
                count+=a;
            }
            this.v3.set(String.valueOf(count));
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "第五步结果求和");
        job.setJarByClass(this.getClass());

        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);



        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MakeSumForMultiplication(),args));
    }
}
