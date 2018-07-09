package org.apache.storm.hbase.bolt.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class FamilyQualiferDataMapper implements Serializable {
	    /**  
	    * @Fields field:field:{todo}  
	    */  
	private static final long serialVersionUID = 1646489125530956001L;
	private final HashMap<String, List<String>> familyMapper = new HashMap<>();
	private final HashMap<String, String> qualiferMapper = new HashMap<>();
	public void add(String family, String qualifier, String dataField) {
		if(familyMapper.containsKey(family)) {
			if(!familyMapper.get(family).contains(qualifier)) {
				familyMapper.get(family).add(qualifier);
			}
			else {
				throw new IllegalArgumentException("qualifer exist in family:" + family);
			}
		}
		else {
			familyMapper.put(family, new ArrayList<String>());
			familyMapper.get(family).add(qualifier);
		}

		if(StringUtils.isNotBlank(qualifier)) {
			qualiferMapper.put(qualifier, dataField);
		}
		else {
			throw new IllegalArgumentException("qualifer could not be null");
		}
	}
	
	
	public HashMap<String, List<String>> getFamilyMapeer(){
		return this.familyMapper;
	}
	
	public HashMap<String, String> getQualiferMapper(){
		return this.qualiferMapper;
	}
	
}
