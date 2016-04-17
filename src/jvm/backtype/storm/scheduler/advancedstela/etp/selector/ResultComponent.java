package backtype.storm.scheduler.advancedstela.etp.selector;

import backtype.storm.scheduler.advancedstela.etp.Component;

public class ResultComponent implements Comparable<ResultComponent>{
    public Component component;
    public Double etpValue;
    public String topologyID;
    public String comparator;
    public Double juiceDist;

    public ResultComponent(Component comp, Double value, String topologyID, String comparator, Double juiceDist) {
        this.component = comp;
        this.etpValue = value;
        this.topologyID = topologyID;
        this.comparator = comparator;
        this.juiceDist = juiceDist;
    }
    
    public ResultComponent(Component comp, Double value, String topologyID, String comparator) {
        this.component = comp;
        this.etpValue = value;
        this.topologyID = topologyID;
        this.comparator = comparator;
        this.juiceDist = -1.0;
    }

    @Override
    public int compareTo(ResultComponent that) {
        //return this.etpValue.compareTo(that.etpValue);
    	switch(comparator){
    	case "EA-JD": //ETP (prioritize) ascending - Juice descending
    		return eajd(that);
    	case "EA-JA":
    		return eaja(that);
    	case "ED-JD":
    		return edjd(that);
    	case "ED-JA":
    		return edja(that);
    	case "JA-ED":
    		return jaed(that);
    	case "JA-EA":
    		return jaea(that);
    	case "JD-ED":
    		return jded(that);
    	case "JD-EA":
    		return jdea(that);
    	default:
    		return this.etpValue.compareTo(that.etpValue);
    }
    }

	private int jdea(ResultComponent that) {
		// TODO Auto-generated method stub
		if(that.getJuiceDistance().compareTo(this.getJuiceDistance())!=0){
			return that.getJuiceDistance().compareTo(this.getJuiceDistance());
		}
		else{
			return this.etpValue.compareTo(that.etpValue);
		}
	}

	private int jded(ResultComponent that) {
		// TODO Auto-generated method stub
		if(that.getJuiceDistance().compareTo(this.getJuiceDistance())!=0){
			return that.getJuiceDistance().compareTo(this.getJuiceDistance());
		}
		else{
			return that.etpValue.compareTo(this.etpValue);
		}
	}

	private int jaea(ResultComponent that) {
		// TODO Auto-generated method stub
		if(that.getJuiceDistance().compareTo(this.getJuiceDistance())!=0){
			return this.getJuiceDistance().compareTo(that.getJuiceDistance());
		}
		else{
			return this.etpValue.compareTo(that.etpValue);
		}
	}

	private int jaed(ResultComponent that) {
		// TODO Auto-generated method stub
		if(that.getJuiceDistance().compareTo(this.getJuiceDistance())!=0){
			return this.getJuiceDistance().compareTo(that.getJuiceDistance());
		}
		else{
			return that.etpValue.compareTo(this.etpValue);
		}
	}

	private int edja(ResultComponent that) {
		// TODO Auto-generated method stub
		if(this.etpValue.compareTo(that.etpValue)!=0){
			return that.etpValue.compareTo(this.etpValue);
		}
		else{
			return this.getJuiceDistance().compareTo(that.getJuiceDistance());
		}
	}

	private int edjd(ResultComponent that) {
		// TODO Auto-generated method stub
		if(this.etpValue.compareTo(that.etpValue)!=0){
			return that.etpValue.compareTo(this.etpValue);
		}
		else{
			return that.getJuiceDistance().compareTo(this.getJuiceDistance());
		}
	}

	private int eaja(ResultComponent that) {
		// TODO Auto-generated method stub
		if(this.etpValue.compareTo(that.etpValue)!=0){
			return this.etpValue.compareTo(that.etpValue);
		}
		else{
			return this.getJuiceDistance().compareTo(that.getJuiceDistance());
		}
	}

	private int eajd(ResultComponent that) {
		// TODO Auto-generated method stub
		if(this.etpValue.compareTo(that.etpValue)!=0){
			return this.etpValue.compareTo(that.etpValue);
		}
		else{
			return that.getJuiceDistance().compareTo(this.getJuiceDistance());
		}
	}
	
	private Double getJuiceDistance() {
		// TODO Auto-generated method stub
		return this.juiceDist;
	}
}
