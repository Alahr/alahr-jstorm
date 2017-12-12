package com.alahr.jstorm.example.joinon.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alahr.jstorm.common.database.ConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class PersonSpout extends BaseRichSpout {
    private final Logger logger = LoggerFactory.getLogger(PersonSpout.class);

    private SpoutOutputCollector collector;

    private Connection connection;

    private boolean flag = false;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
        connection = ConnectionUtil.openMysqlConn("/database.properties");
    }

    public void nextTuple(){
        if(flag){
            return;
        }
        Statement stmt = null;
        try{
            stmt = connection.createStatement();
            String sql = "select * from person";
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()){
                String pNo = rs.getString("p_no");
                String pName = rs.getString("p_name");
                int pAge = rs.getInt("p_age");
                String pAddress = rs.getString("p_address");

                Map<String, Object> map = new HashMap<String, Object>();
                map.put("pNo", pNo);
                map.put("pName", pName);
                map.put("pAge", pAge);
                map.put("pAddress", pAddress);

                logger.info(map.toString());

                this.collector.emit("person", new Values(pNo, map));
            }
        }
        catch (SQLException e){
            logger.error("create statement", e);
        }
        finally{
            flag = true;
            ConnectionUtil.closeStatement(stmt);
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("person", new Fields("joinKey", "rowInfo"));
    }

    public void close(){
        ConnectionUtil.closeMysqlConn(connection);
    }
}
