package com.briup.bigdata.project.grms.utils.step5;

        import org.apache.hadoop.io.WritableComparable;
        import org.apache.hadoop.io.WritableComparator;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step5
 * @CreateDate: 2019/10/12 17:02
 * @Description:
 */
public class MySortComparator extends WritableComparator {
    public MySortComparator() {
        super(TupleKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return super.compare(a, b);
    }
}
