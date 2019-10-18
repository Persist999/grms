package com.briup.bigdata.project.grms.utils.step5;



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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {

    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper
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
   public static class MultiplyGoodsMatrixAndUserVectorSecondMapper
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

   public static class MultiplyGoodsMatrixAndUserVectorReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {

            List<Text> list=new ArrayList();
            Map<String,Integer> map1=new HashMap<>();
            Map<String,Integer> map2=new HashMap<>();
            v2s.forEach(val->{
                list.add(new Text(val));
            });
            String[] str1 = list.get(0).toString().split("[,]");
            String[] str2 = list.get(1).toString().split("[,]");
            for (String s : str1) {
                String[] str3 = s.split("[:]");
                map1.put(str3[0],Integer.parseInt(str3[1]));
            }
            for (String s : str2) {
                String[] str3 = s.split("[:]");
                map2.put(str3[0],Integer.parseInt(str3[1]));
            }
            for (Map.Entry<String,Integer> v: map1.entrySet()){
                for (Map.Entry<String,Integer> o: map2.entrySet()){
                    System.out.println(";;;;"+v.getKey());
                    if (v.getKey().substring(0,1).equals("1")){
                        System.out.println("::::"+v.getKey());
                        this.k3.set(v.getKey() + "," + o.getKey());
                        System.out.println("....."+v.getKey());}

                    else {this.k3.set(o.getKey()+","+v.getKey());}

                    this.v3.set(String.valueOf(v.getValue() * o.getValue()));
                    context.write(this.k3, this.v3);
                }
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in1=new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));

        Path out=new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "物品的共现矩阵");
        job.setJarByClass(this.getClass());


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,in1,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);



        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
}
