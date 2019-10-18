package com.briup.bigdata.project.grms.utils.run;

import com.briup.bigdata.project.grms.utils.db.SaveRecommendResultToDB;
import com.briup.bigdata.project.grms.utils.db.UidGidEmp;
import com.briup.bigdata.project.grms.utils.step1.UserBuyGoodsList;
import com.briup.bigdata.project.grms.utils.step2.GoodsCooccurrenceList;
import com.briup.bigdata.project.grms.utils.step3.GoodsCooccurrenceMatrix;
import com.briup.bigdata.project.grms.utils.step4.UserBuyGoodsVector;
import com.briup.bigdata.project.grms.utils.step5.MultiplyGoodsMatrixAndUserVector;
import com.briup.bigdata.project.grms.utils.step6.MakeSumForMultiplication;
import com.briup.bigdata.project.grms.utils.step7.DuplicateDataForResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils
 * @CreateDate: 2019/10/12 10:29
 * @Description: 作业流
 */
public class GoodsRecommendationManagementSystemJobController extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=this.getConf();

        Path in=new Path(conf.get("in"));//作业1的输入原始数据，作业4的输入，作业7的输入
        Path out1=new Path(conf.get("out1"));//作业1的输出，作业2的输入
        Path out2=new Path(conf.get("out2"));//作业2的输出，作业3的输入
        Path out3=new Path(conf.get("out3"));//作业3的输出，作业5的输入
        Path out4=new Path(conf.get("out4"));//作业4的输出，作业5的输入
        Path out5=new Path(conf.get("out5"));//作业5的输出，作业6的输入
        Path out6=new Path(conf.get("out6"));//作业6的输出，作业7的输入
        Path out7=new Path(conf.get("out7"));//作业7的输出，作业8的输入


        Job job1 = Job.getInstance(conf, "作业1：计算用户购买的商品的列表");
        job1.setJarByClass(this.getClass());

        job1.setMapperClass(UserBuyGoodsList.UserBuyGoodsListMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,in);

        job1.setReducerClass(UserBuyGoodsList.UserBuyGoodsListReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,out1);

        //--------------------------------------------------------------------

        Job job2 = Job.getInstance(conf, "作业2：计算商品共现关系");
        job2.setJarByClass(this.getClass());

        job2.setMapperClass(GoodsCooccurrenceList.GoodsCooccurrenceListMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,out1);

        job2.setReducerClass(GoodsCooccurrenceList.GoodsCooccurrenceListReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,out2);

        //--------------------------------------------------------------------

        Job job3 = Job.getInstance(conf, "作业3：计算商品共现关系(共线矩阵)");
        job3.setJarByClass(this.getClass());

        job3.setMapperClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,out2);

        job3.setReducerClass(GoodsCooccurrenceMatrix.GoodsCooccurrenceMatrixReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3,out3);

        //--------------------------------------------------------------------

        Job job4 = Job.getInstance(conf, "作业4：计算用户的购买向量");
        job4.setJarByClass(this.getClass());

        job4.setMapperClass(UserBuyGoodsVector.UserBuyGoodsVectorMapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,in);

        job4.setReducerClass(UserBuyGoodsVector.UserBuyGoodsVectorReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4,out4);

        //--------------------------------------------------------------------

        Job job5 = Job.getInstance(conf, "作业5：物品的共现矩阵");
        job5.setJarByClass(this.getClass());

        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job5,out3,TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job5,out4,TextInputFormat.class, MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        job5.setReducerClass(MultiplyGoodsMatrixAndUserVector.MultiplyGoodsMatrixAndUserVectorReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job5,out5);

        //--------------------------------------------------------------------

        Job job6 = Job.getInstance(conf, "作业6：第五步结果求和");
        job6.setJarByClass(this.getClass());

        job6.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
        job6.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job6,out5);

        job6.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job6,out6);

        //--------------------------------------------------------------------

        Job job7 = Job.getInstance(conf, "作业7：数据去重");
        job7.setJarByClass(this.getClass());

        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job7,in,TextInputFormat.class,
                DuplicateDataForResult.DuplicateDataForResultFirstMapper.class);
        MultipleInputs.addInputPath(job7,out6,TextInputFormat.class,
                DuplicateDataForResult.DuplicateDataForResultSecondMapper.class);

        job7.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job7,out7);

        //--------------------------------------------------------------------

        Job job8 = Job.getInstance(conf, "作业8：存入数据库");
        job8.setJarByClass(this.getClass());

        job8.setMapperClass(SaveRecommendResultToDB.SaveRecommendResultToDBMapper.class);
        job8.setMapOutputKeyClass(UidGidEmp.class);
        job8.setMapOutputValueClass(NullWritable.class);
        job8.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job8,out7);

        job8.setOutputKeyClass(UidGidEmp.class);
        job8.setOutputValueClass(NullWritable.class);
        job8.setOutputFormatClass(DBOutputFormat.class);

        DBConfiguration.configureDB(job8.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://master:3306/grms","root","123456");
        DBOutputFormat.setOutput(job8,"results",
                "uid","gid","exp");

        //--------------------------------------------------------------------

        ControlledJob cj1=new ControlledJob(conf);
        cj1.setJob(job1);

        ControlledJob cj2=new ControlledJob(conf);
        cj2.setJob(job2);

        ControlledJob cj3=new ControlledJob(conf);
        cj3.setJob(job3);

        ControlledJob cj4=new ControlledJob(conf);
        cj4.setJob(job4);

        ControlledJob cj5=new ControlledJob(conf);
        cj5.setJob(job5);

        ControlledJob cj6=new ControlledJob(conf);
        cj6.setJob(job6);

        ControlledJob cj7=new ControlledJob(conf);
        cj7.setJob(job7);

        ControlledJob cj8=new ControlledJob(conf);
        cj8.setJob(job8);

        cj2.addDependingJob(cj1);

        cj3.addDependingJob(cj2);


        cj5.addDependingJob(cj3);
        cj5.addDependingJob(cj4);

        cj6.addDependingJob(cj5);

        cj7.addDependingJob(cj6);

        cj8.addDependingJob(cj7);
        JobControl jc=new JobControl("作业流控制");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);

        //提交作业
        Thread t=new Thread(jc);
        t.start();

        do {
            for (ControlledJob j:jc.getRunningJobList()){
                j.getJob().monitorAndPrintJob();
            }

        }while (!jc.allFinished());

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsRecommendationManagementSystemJobController(),args));
    }
}
