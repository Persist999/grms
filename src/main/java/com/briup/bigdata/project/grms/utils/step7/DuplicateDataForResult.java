package com.briup.bigdata.project.grms.utils.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
 * @CreateDate: 2019/10/11 15:24
 * @Description:
 */
public class DuplicateDataForResult extends Configured implements Tool {
    public static class DuplicateDataForResultFirstMapper
            extends Mapper<LongWritable, Text,Text,Text> {
        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] str=v1.toString().split("[\t]");
            this.k2.set(str[0]+"\t"+str[1]);
            this.v2.set(str[2]);
            context.write(this.k2,this.v2);
        }
    }
    public static class DuplicateDataForResultSecondMapper
            extends Mapper<LongWritable, Text,Text,Text> {
        private Text k2=new Text();
        private Text v2=new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] str=v1.toString().split("[\t]");
            String[] str2 = str[0].split("[,]");
            this.k2.set(str2[0]+"\t"+str2[1]);
            this.v2.set(str[1]);
            context.write(this.k2,this.v2);
        }
    }
    public static class DuplicateDataForResultReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            this.k3.set(k2.toString());
            List<Text> list=new ArrayList();
            v2s.forEach(val->{
                list.add(new Text(val.toString()));
            });
//            if (list.size()>=2) {
//                int num1 = Integer.parseInt(list.get(0).toString()) - Integer.parseInt(list.get(1).toString());
//                int num = Math.abs(num1);
//                if (num!=0){
//
//                    this.v3.set(String.valueOf(num));
//                    context.write(this.k3,this.v3);
//                }
//            }else {
//
//                this.v3.set(list.get(0).toString());
//                context.write(this.k3,this.v3);}
            if (list.size()<2){
                this.v3.set(list.get(0).toString());
                context.write(this.k3,this.v3);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1=new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));

        Path out=new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "数据去重");
        job.setJarByClass(this.getClass());


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,
                DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,
                DuplicateDataForResultSecondMapper.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);



        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
}
