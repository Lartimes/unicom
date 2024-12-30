package com.lartimes.unicom.mapreduce.bean;

import lombok.Data;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 22:11
 */
@Data
public class Unicom implements Serializable, Writable, DBWritable {
    private String imsi;
    private LocalDateTime timeNow;
    private String net;
    private String sex;
    private Integer ageWeight;
    private Integer arpu;

    private String brand;

    private String model;

    private Integer trafficWeight;

    private Integer callSum;

    private Integer smsTotal;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(imsi);
        out.writeUTF(timeNow.toString());
        out.writeUTF(net);
        out.writeUTF(sex);
        out.writeInt(ageWeight);
        out.writeInt(arpu);
        out.writeUTF(brand);
        out.writeUTF(model);
        out.writeInt(trafficWeight);
        out.writeInt(callSum);
        out.writeInt(smsTotal);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.imsi = in.readUTF();
        this.timeNow = LocalDateTime.parse(in.readUTF());
        this.net = in.readUTF();
        this.sex = in.readUTF();
        this.ageWeight = in.readInt();
        this.arpu = in.readInt();
        this.brand = in.readUTF();
        this.model = in.readUTF();
        this.trafficWeight = in.readInt();
        this.callSum = in.readInt();
        this.smsTotal = in.readInt();
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1, imsi);
        ps.setString(2, timeNow.toString());
        ps.setString(3, net);
        ps.setString(4, sex);
        ps.setInt(5, ageWeight);
        ps.setInt(6, arpu);
        ps.setString(7, brand);
        ps.setString(8, model);
        ps.setInt(9, trafficWeight);
        ps.setInt(10, callSum);
        ps.setInt(11, smsTotal);
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        this.imsi = rs.getString(1);
        this.timeNow = LocalDateTime.parse(rs.getString(2));
        this.net = rs.getString(3);
        this.sex = rs.getString(4);
        this.ageWeight = rs.getInt(5);
        this.arpu = rs.getInt(6);
        this.brand = rs.getString(7);
        this.model = rs.getString(8);
        this.trafficWeight = rs.getInt(9);
        this.callSum = rs.getInt(10);
        this.smsTotal = rs.getInt(11);
    }

}

