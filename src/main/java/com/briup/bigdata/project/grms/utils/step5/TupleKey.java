package com.briup.bigdata.project.grms.utils.step5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.step5
 * @CreateDate: 2019/10/12 16:29
 * @Description:
 */
public class TupleKey implements WritableComparable<TupleKey> {
    private Text gid;
    private Text flag;

    public TupleKey() {
        this.gid=new Text();
        this.flag=new Text();
    }

    @Override
    public int compareTo(TupleKey o) {
        int gd=this.gid.compareTo(o.gid);
        int fg=this.flag.compareTo(o.flag);
        return gd==0?fg:gd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(gid,flag);
    }

    @Override
    public boolean equals(Object obj) {
        if (this==obj) return true;
        if (!(obj instanceof TupleKey)) return false;
        TupleKey tupleKey= (TupleKey) obj;
        return getGid().equals(tupleKey.getGid())&&getFlag().equals(tupleKey.getFlag());
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.gid.write(dataOutput);
        this.gid.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gid.readFields(dataInput);
        this.flag.readFields(dataInput);

    }

    public Text getGid() {
        return gid;
    }

    public void setGid(Text gid) {
        this.gid.set(gid.toString());
    }
    public void setGid(String gid) {
        this.gid.set(gid);
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag.set(flag.toString());
    }
    public void setFlag(String flag) {
        this.flag.set(flag);
    }
}
