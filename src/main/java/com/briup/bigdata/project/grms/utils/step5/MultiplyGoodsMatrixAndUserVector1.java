package com.briup.bigdata.project.grms.utils.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step5
 * @CreateDate: 2019/10/12 15:24
 * @Description:
 */



public class MultiplyGoodsMatrixAndUserVector1 extends Configured implements Tool {

    public static class MultiplyGoodsMatrixAndUserVector1FirstMapper
            extends Mapper<Text, Text,TupleKey,Text> {
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {

            System.out.println("k1的值为:"+k1.toString()+"\t"+k1);
            this.k2.setGid(k1.toString());
            this.k2.setFlag("0");
            this.v2.set(v1.toString());
            System.out.println("v1的值为:"+v1.toString()+"\t"+v1);
            context.write(this.k2,this.v2);
        }
    }
    public static class MultiplyGoodsMatrixAndUserVector1SecondMapper
            extends Mapper<Text, Text,TupleKey,Text> {
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1, Text v1, Context context) throws IOException, InterruptedException {
            System.out.println("k1的值为:"+k1.toString()+"\t"+k1);
            this.k2.setGid(k1.toString());
            this.k2.setFlag("1");
            this.v2.set(v1.toString());
            System.out.println("v1的值为:"+v1.toString()+"\t"+v1);
            context.write(this.k2,this.v2);
        }
    }

    public static class MultiplyGoodsMatrixAndUserVector1Reducer
            extends Reducer<TupleKey,Text,Text,Text> {
        private Text k3=new Text();
        private Text v3=new Text();

        @Override
        protected void reduce(TupleKey k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            Iterator<Text> it=v2s.iterator();

            String str1 = new Text(it.next()).toString();
            System.out.println(str1);
            String str2 = new Text(it.next()).toString();
            System.out.println(str2);

            String[] gms = str1.split("[,]");
            String[] uvs = str2.split("[,]");

            for (String gm : gms) {
                String[] gmis = gm.split("[:]");
                for (String uv : uvs) {
                    String[] uvis = uv.split("[:]");
                    this.k3.set(uvis[0]+","+gmis[0]);
                    this.v3.set(String.valueOf(
                            Integer.parseInt(gmis[1])* Integer.parseInt(uvis[1])));
                    context.write(this.k3,this.v3);
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


        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,in1,KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVector1FirstMapper.class);
        MultipleInputs.addInputPath(job,in2,KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVector1SecondMapper.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVector1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setSortComparatorClass(MySortComparator.class);



        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector1(),args));
    }
}
