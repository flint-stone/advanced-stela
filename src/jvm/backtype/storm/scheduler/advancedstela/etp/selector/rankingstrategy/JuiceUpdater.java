package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.slo.Topology;

public class JuiceUpdater {
	
	public static Double juiceUpadate(TopologySchedule ts, TreeMap<String, Double> expectedEmitRate, TreeMap<String, Double> expectedExecutedRates, ArrayList<Component> sourceList){
		//add a juice list for each component
		HashMap<String, HashMap<String, Double>> juiceMap = new HashMap<String, HashMap<String, Double>>();
		//Queue<String> componentBFS = new Queue<String>();
		ArrayList<String> componentBFS = new ArrayList<String>();
		ArrayList<String> parents = new ArrayList(sourceList);
		ArrayList<String> children = new ArrayList<String>();
		ArrayList<String> sinks = new ArrayList<String>();
		
		while(parents.size()!=0){
			for(String parent:parents){
				Component pComponent = ts.getComponents().get(parent);
				for(String child:pComponent.getChildren()){
					children.add(child);
				}
			}
			componentBFS.addAll(parents);
			parents = children;
			children = new ArrayList<String>();
		}
		for(String component:componentBFS){
			updateJuicePerSource(ts, expectedEmitRate, expectedExecutedRates, sourceList, juiceMap, componentBFS, component);
			if(ts.getComponents().get(component).getChildren().size() ==0){
				sinks.add(component);
			}
		}
		
		//collect juice by traverse all sinks
		Double rawScore =0.0;
		for(String sink : sinks){
			for(Double score: juiceMap.get(sink).values()){
				rawScore+=score;
			}
			
		}
		
		return rawScore/sourceList.size();
	}
	
	private static void updateJuicePerSource(TopologySchedule ts, TreeMap<String, Double> expectedEmitRate, TreeMap<String, Double> expectedExecutedRates, ArrayList<Component> sourceList, HashMap<String, HashMap<String, Double>> juiceMap, ArrayList<String> componentBFS, String component) {
		//if component is a source
		Component c = ts.getComponents().get(component);
		HashMap<String, Double> newCompEntry = new HashMap<String, Double>();
		if(c.getParents().size()==0){
			for(int i = 0;i<sourceList.size(); i++){
				newCompEntry.put(sourceList.get(i).getId(), 1.0);
			}
		}
		else{// if component is not a source
			List<String> parents = c.getParents();
			Double remainingRate = expectedExecutedRates.get(c.getId())/expectedEmitRate.get(c.getId());
			for(Component source: sourceList){
				Double parentSourceScore = 0.0;
				for(String parent: parents){
					int childrensize = ts.getComponents().get(parent).getChildren().size();
					parentSourceScore+=juiceMap.get(parent).get(source)*remainingRate/childrensize;
				}
				newCompEntry.put(source.getId(), parentSourceScore);
			}			
		}		
		juiceMap.put(component, newCompEntry);
	}

	
}
