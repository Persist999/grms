package com.briup.bigdata.project.grms.utils.step5;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.util.Comparator;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step5
 * @CreateDate: 2019/10/12 16:55
 * @Description:
 */
public class MyGroupComparator extends WritableComparator {

    public MyGroupComparator() {
        super(TupleKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TupleKey tka= (TupleKey) a;
        TupleKey tkb= (TupleKey) b;
        return super.compare(tka.getGid(), tkb.getGid());
    }
}
