package org.apache.storm.hbase.bolt.mapper;

import static org.apache.storm.hbase.common.Utils.toBytes;
import static org.apache.storm.hbase.common.Utils.toLong;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.common.ColumnList;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ComplexHbaseMapper implements HBaseMapper {
    private String rowKeyField;
//  private String timestampField;
    private FamilyQualiferDataMapper FQDMapper;
	
    public ComplexHbaseMapper() {
    	FQDMapper = new FamilyQualiferDataMapper();
    }
    public ComplexHbaseMapper withRowKeyField(String rowKeyField){
        this.rowKeyField = rowKeyField;
        return this;
    }

    public ComplexHbaseMapper withColumnFields(String family, String qualifier, String dataField){
        this.FQDMapper.add(family, qualifier, dataField);
        return this;
    }

    
    @Override
	public byte[] rowKey(Tuple tuple) {
		// TODO Auto-generated method stub
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return toBytes(objVal);
	}

	@Override
	public ColumnList columns(Tuple tuple) {
		// TODO Auto-generated method stub
        ColumnList cols = new ColumnList();
        FamilyQualiferDataMapper temp = FQDMapper;
        for(String family: temp.getFamilyMapeer().keySet()) {
        	List<String> qualifierList = temp.getFamilyMapeer().get(family);
        	for(String qualifer : qualifierList) {
        		cols.addColumn(toBytes(family), toBytes(qualifer)
        			, toBytes(tuple.getValueByField(temp.getQualiferMapper().get(qualifer))));
        	}
        	
        }
        return cols;
	}

}
