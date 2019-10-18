package com.briup.bigdata.project.grms.utils.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.db
 * @CreateDate: 2019/10/12 10:02
 * @Description:
 */
public class SaveRecommendResultToDB extends Configured implements Tool {
   public static class SaveRecommendResultToDBMapper
        extends Mapper<LongWritable, Text,UidGidEmp, NullWritable> {
    private UidGidEmp k2=new UidGidEmp();
    private NullWritable v2=NullWritable.get();

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String[] strs = v1.toString().split("[\t]");
        this.k2.setUid(strs[0]);
        this.k2.setGid(strs[1]);
        this.k2.setExp(Integer.parseInt(strs[2]));
        context.write(this.k2,this.v2);

    }
}


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();
        Path in=new Path(conf.get("in"));

        Job job = Job.getInstance(conf, "存入数据库");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        job.setMapOutputKeyClass(UidGidEmp.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setOutputKeyClass(UidGidEmp.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://master:3306/grms","root","123456");
        DBOutputFormat.setOutput(job,"results",
                "uid","gid","exp");


        System.out.println("配置任务已完成");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
}
