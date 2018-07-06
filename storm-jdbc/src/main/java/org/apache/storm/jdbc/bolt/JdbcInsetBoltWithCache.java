package org.apache.storm.jdbc.bolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
/*import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class JdbcInsetBoltWithCache extends AbstractJdbcBolt{
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInsertBolt.class);

    private String tableName;
    private String insertQuery;
    private JdbcMapper jdbcMapper;
    private transient OverTimeTask overtimeTask;
    private long timeInterval = -1L;
    private Integer cacheSize;

    public JdbcInsetBoltWithCache(ConnectionProvider connectionProvider,  JdbcMapper jdbcMapper) {
        super(connectionProvider);
        Validate.notNull(jdbcMapper);
        this.jdbcMapper = jdbcMapper;
    }

    public JdbcInsetBoltWithCache withTableName(String tableName) {
        if (insertQuery != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        }
        this.tableName = tableName;
        return this;
    }

    public JdbcInsetBoltWithCache setCacheSize(int cacheSize) {
    	this.cacheSize = cacheSize;
    	return this;
    }
    
    public JdbcInsetBoltWithCache setTimeInterval(long timeInterval) {
    	this.timeInterval = timeInterval;
    	return this;
    }
    
    public JdbcInsetBoltWithCache withInsertQuery(String insertQuery) {
        if (this.tableName != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        }
        this.insertQuery = insertQuery;
        return this;
    }

    public JdbcInsetBoltWithCache withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        if(this.cacheSize == null || this.cacheSize <= 0) {
        	throw new IllegalArgumentException("cache must larger than 1 and could not be null");
        }
        this.jdbcClient.setRowCache(this.cacheSize);
        if(StringUtils.isBlank(tableName) && StringUtils.isBlank(insertQuery)) {
            throw new IllegalArgumentException("You must supply either a tableName or an insert Query.");
        }
        if(this.timeInterval > 0) {
        	Timer overtimeTimer = new Timer();
        	overtimeTask = new OverTimeTask();
        	overtimeTask.setTimeInterval(this.timeInterval);
        	overtimeTimer.schedule(overtimeTask,new Date(), this.timeInterval);
        }
    }

    protected void process(Tuple tuple) {
        try {
            List<Column> columns = jdbcMapper.getColumns(tuple);
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
            if(!StringUtils.isBlank(tableName)) {
                this.jdbcClient.insertWithCache(this.tableName, columnLists);
            } else {
            	this.jdbcClient.executeInsertQueryWithCache(this.insertQuery, columnLists);
        		this.overtimeTask.setOvertimeFlag(false);
        		this.overtimeTask.setLastTs(System.currentTimeMillis());
        		this.overtimeTask.setOvertimeFlag(true);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    
    @Override
    public void cleanup() {
    	this.jdbcClient.closeLongConnection();
    	super.cleanup();

    }
    private class OverTimeTask extends TimerTask{
    	public long getLastTs() {
			return lastTs;
		}
		public void setLastTs(long lastTs) {
			this.lastTs = lastTs;
		}
		public long getTimeInterval() {
			return timeInterval;
		}
		public void setTimeInterval(long timeInterval) {
			this.timeInterval = timeInterval;
		}
		public boolean isOvertimeFlag() {
			return overtimeFlag;
		}
		public void setOvertimeFlag(boolean overtimeFlag) {
			this.overtimeFlag = overtimeFlag;
		}
		private long lastTs;
    	private long timeInterval = -1L;
    	private boolean overtimeFlag =false;
		@Override
		public void run() {
			// TODO Auto-generated method stub
			long current = System.currentTimeMillis();
			if(timeInterval > 0 && overtimeFlag && (current - lastTs) > timeInterval) {
				jdbcClient.LongConnectionCommit();
				overtimeFlag = false;
				jdbcClient.closeLongConnection();
			}
			
		}
    	
    }

    
    
}
