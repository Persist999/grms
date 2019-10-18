package com.briup.bigdata.project.grms.utils.step3;

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
import java.util.HashMap;
import java.util.Map;

public class GoodsCooccurrenceMatrix extends Configured implements Tool {
   public static class GoodsCooccurrenceMatrixMapper
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
    public static class GoodsCooccurrenceMatrixReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
        this.k3.set(k2.toString());
        Map<Text, Integer> map=new HashMap();
        v2s.forEach(val->{
            map.put(new Text(val.toString()),map.get(val)==null?1:map.get(val)+1);
        });
        StringBuilder sb=new StringBuilder();
       for(Map.Entry<Text,Integer> v:map.entrySet()){
            sb.append(v.getKey()).append(":").append(v.getValue()).append(",");

       }
            this.v3.set(sb.substring(0,sb.length()-1));
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));
        Path out=new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "计算商品共现关系(共线矩阵)");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);



        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
}
