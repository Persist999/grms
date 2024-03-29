package com.briup.bigdata.project.grms.utils.db;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Simple to Introduction
 *
 * @author DXQ
 * @ProjectName: GRMS
 * @PackageName:com.briup.bigdata.project.grms.utils.db
 * @CreateDate: 2019/10/12 09:24
 * @Description: 自定义数据类型，方便数据写入数据库
 */
public class UidGidEmp implements DBWritable, WritableComparable<UidGidEmp> {

    private Text uid;
    private Text gid;
    private IntWritable exp;

    public UidGidEmp() {
        this.uid=new Text();
        this.gid=new Text();
        this.exp=new IntWritable();
    }
    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1,this.uid.toString());
        ps.setString(2,this.gid.toString());
        ps.setInt(3,this.exp.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid,gid,exp);
    }

    @Override
    public boolean equals(Object o) {
        if (this==o) return true;
        if (!(o instanceof UidGidEmp)) return false;
        UidGidEmp that= (UidGidEmp) o;
        return uid.equals(that.uid)&&gid.equals(that.gid)&&exp.equals(that.exp);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        this.uid.set(rs.getString(1));
        this.gid.set(rs.getString(2));
        this.exp.set(rs.getInt(3));
    }

    @Override
    public int compareTo(UidGidEmp o) {
        int ud=this.uid.compareTo(o.uid);
        int gd=this.gid.compareTo(o.gid);
        int ep=this.exp.compareTo(o.exp);

        return ud==0?(gd==0?ep:gd):ud;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            this.uid.write(dataOutput);
            this.gid.write(dataOutput);
            this.exp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.uid.readFields(dataInput);
            this.gid.readFields(dataInput);
            this.exp.readFields(dataInput);
    }

    public Text getUid() {
        return uid;
    }

    public void setUid(Text uid) {
        this.uid.set(uid.toString());
    }
    public void setUid(String uid) {
        this.uid.set(uid);
    }

    public Text getGid() {
        return gid;
    }
    public void setGid(String gid) {
        this.gid.set(gid);
    }
    public void setGid(Text gid) {
        this.gid.set(gid.toString());
    }

    public IntWritable getExp() {
        return exp;
    }
    public void setExp(int exp) {
        this.exp.set(exp);
    }
    public void setExp(IntWritable exp) {
        this.exp.set(exp.get());
    }

    @Override
    public String toString() {
        return this.uid+"\t"+this.gid+"\t"+this.exp;
    }
}
