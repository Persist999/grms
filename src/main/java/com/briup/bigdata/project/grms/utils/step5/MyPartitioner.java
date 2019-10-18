package com.briup.bigdata.project.grms.utils.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step5
 * @CreateDate: 2019/10/12 16:52
 * @Description:
 */
public class MyPartitioner extends Partitioner<TupleKey, Text> {
    @Override
    public int getPartition(TupleKey tupleKey, Text text, int i) {
        return tupleKey.getGid().hashCode()%i;
    }
}
