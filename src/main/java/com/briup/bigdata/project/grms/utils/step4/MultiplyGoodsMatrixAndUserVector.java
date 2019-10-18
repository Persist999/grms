package com.briup.bigdata.project.grms.utils.step4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @program: grms.pre
 * @package: com.briup.bigdata.bd1903.project.grms.step5
 * @filename: MultiplyGoodsMatrixAndUserVector.java
 * @create: 2019.10.12 09:54
 * @author: Kevin
 * @description: .5.商品共现矩阵乘以用户购买向量，形成临时的推荐结果。
 **/
public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool{
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }

    @Override
    public int run(String[] strings) throws Exception{
        Configuration conf=this.getConf();
        Path in1=new Path("/grms/rawdata/3.7/part-r-00000");
        Path in2=new Path("/grms/rawdata/4/part-r-00000");
        Path out=new Path("/grms/rawdata/5.5.5");

        Job job=Job.getInstance(conf,"作业5：商品共现矩阵乘以用户购买向量，形成临时的推荐结果");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(job,in1,KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job,in2,KeyValueTextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job.setMapOutputKeyClass(TupleKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setSortComparatorClass(MySortComparator.class);

        return job.waitForCompletion(true)?0:1;
    }

    // 处理物品的共现矩阵数据
    static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<Text,Text,TupleKey,Text>{
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1,Text v1,Context context) throws IOException, InterruptedException{
            this.k2.setGid(k1.toString());
            this.k2.setFlag("0");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);
        }
    }

    // 处理用户的购买向量数据
    static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<Text,Text,TupleKey,Text>{
        private TupleKey k2=new TupleKey();
        private Text v2=new Text();

        @Override
        protected void map(Text k1,Text v1,Context context) throws IOException, InterruptedException{
            this.k2.setGid(k1.toString());
            this.k2.setFlag("1");
            this.v2.set(v1.toString());
            context.write(this.k2,this.v2);
        }
    }

    static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<TupleKey,Text,Text,IntWritable>{
        private Text k3=new Text();
        private IntWritable v3=new IntWritable();

        @Override
        protected void reduce(TupleKey k2,Iterable<Text> v2s,Context context) throws IOException, InterruptedException{
            Iterator<Text> it=v2s.iterator();

            String gm=new Text(it.next()).toString();
            String uv=new Text(it.next()).toString();

            String[] gms=gm.split("[,]");
            String[] uvs=uv.split("[,]");

            for(String gmi: gms){ // 20001:3
                String[] gmis=gmi.split("[:]");
                for(String uvi: uvs){ // 10001:1
                    String[] uvis=uvi.split("[:]");
                    this.k3.set(uvis[0]+","+gmis[0]);
                    this.v3.set(Integer.parseInt(gmis[1])*Integer.parseInt(uvis[1]));
                    context.write(this.k3,this.v3);
                }
            }
        }
    }
}

class TupleKey implements WritableComparable<TupleKey>{
    private Text gid;
    private Text flag;

    public TupleKey(){
        this.gid=new Text();
        this.flag=new Text();
    }

    @Override
    public int hashCode(){
        return Objects.hash(getGid(),getFlag());
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof TupleKey)) return false;
        TupleKey tupleKey=(TupleKey)o;
        return getGid().equals(tupleKey.getGid())&&getFlag().equals(tupleKey.getFlag());
    }

    @Override
    public String toString(){
        return this.gid+"\t"+this.flag;
    }

    public Text getGid(){
        return gid;
    }

    public void setGid(Text gid){
        this.gid.set(gid.toString());
    }

    public void setGid(String gid){
        this.gid.set(gid);
    }

    public Text getFlag(){
        return flag;
    }

    public void setFlag(Text flag){
        this.flag.set(flag.toString());
    }

    public void setFlag(String flag){
        this.flag.set(flag);
    }

    @Override
    public int compareTo(TupleKey o){
        int gidComp=this.gid.compareTo(o.gid);
        int flagComp=this.flag.compareTo(o.flag);
        return gidComp==0?flagComp:gidComp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException{
        this.gid.write(dataOutput);
        this.flag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException{
        this.gid.readFields(dataInput);
        this.flag.readFields(dataInput);
    }
}

class MyPartitioner extends Partitioner<TupleKey,Text>{
    @Override
    public int getPartition(TupleKey tupleKey,Text text,int i){
        return tupleKey.getGid().hashCode()%i;
    }
}

class MyGroupComparator extends WritableComparator{
    public MyGroupComparator(){
        super(TupleKey.class,true);
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b){
        TupleKey tka=(TupleKey)a;
        TupleKey tkb=(TupleKey)b;
        return super.compare(tka.getGid(),tkb.getGid());
    }
}

class MySortComparator extends WritableComparator{
    public MySortComparator(){
        super(TupleKey.class,true);
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b){
        return super.compare(a,b);
    }
}
