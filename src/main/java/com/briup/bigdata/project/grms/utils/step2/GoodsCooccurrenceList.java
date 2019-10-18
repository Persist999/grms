package com.briup.bigdata.project.grms.utils.step2;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GoodsCooccurrenceList extends Configured implements Tool {
       public static class GoodsCooccurrenceListMapper
                extends Mapper<LongWritable, Text,Text,Text> {
            private Text k2=new Text();
            private Text v2=new Text();

            @Override
            protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
                String[] str=v1.toString().split("[\t]");
                String[] sb=str[1].split("[,]");
                for (String s : sb) {
                    this.k2.set(s);
                    for (String s1 : sb) {

                        this.v2.set(s1);
                        context.write(this.k2,this.v2);
                    }

                }

            }
        }
        public static class GoodsCooccurrenceListReducer
                extends Reducer<Text,Text,Text,Text> {
            private Text k3=new Text();
            private Text v3=new Text();

            @Override
            protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
                this.k3.set(k2.toString());
                v2s.forEach(val->{
                    this.v3.set(val);
                    try {
                        context.write(this.k3,this.v3);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        }

        @Override
        public int run(String[] strings) throws Exception {
            Configuration conf=this.getConf();
            Path in=new Path(conf.get("in"));
            Path out=new Path(conf.get("out"));

            Job job = Job.getInstance(conf, "计算商品共现关系");
            job.setJarByClass(this.getClass());

            job.setMapperClass(GoodsCooccurrenceListMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job,in);

            job.setReducerClass(GoodsCooccurrenceListReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,out);



            System.out.println("配置任务已完成");

            return job.waitForCompletion(true)?0:1;
        }

        public static void main(String[] args) throws Exception {
            System.exit(ToolRunner.run(new GoodsCooccurrenceList(),args));
        }
}


