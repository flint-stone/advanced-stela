package backtype.storm.scheduler.advancedstela.etp.selector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.advancedstela.etp.Component;
import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.GlobalStatistics;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPCalculation;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPFluidPredictionStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy;
import backtype.storm.scheduler.advancedstela.slo.Observer;

public class FluidPredictionSelector implements Selector {

	private HashMap<String, TopologySchedule> sbTopoScheds;
	private HashMap<String, TopologyStatistics> sbTopoStats;
	ArrayList<String> sbTargets;
	ArrayList<String> sbVictims;
	ArrayList<String> sbAllComps;
	private HashMap<String, TreeMap<String, Double> > sbTopoEmitRates;
	private HashMap<String, TreeMap<String, Double> > sbTopoExecRates;
	private HashMap<String, HashMap<Component, Double>> sbCongestionMap;
	private HashMap<String, HashMap<String, Integer>> sbParaMap;
	private HashMap<String, ArrayList<Component>> sourceListMap;
	@Override
	public ArrayList<ExecutorPair> selectPairs(GlobalState globalState, GlobalStatistics globalStatistics, ArrayList<String> targetIDs, ArrayList<String> victimIDs, Observer sloObserver){
        //deep copy globalstatistics and globalstate
        //GlobalState sbState = new GlobalState(globalState.getConfig(), snimbusClient, File advanced_scheduling_log, HashMap<String, TopologySchedule> topologySchedules, HashMap<String, Node> supervisorToNode)
        sbTopoScheds = new HashMap<String, TopologySchedule>();
        sbTopoStats = new HashMap<String, TopologyStatistics>();
        this.sbTopoScheds.putAll(globalState.getTopologySchedules());
        this.sbTopoStats.putAll(globalStatistics.getTopologyStatistics());
        sbTargets = new ArrayList<String>();
        sbVictims = new ArrayList<String>();
        sbTargets.addAll(sbTargets); 
        sbVictims.addAll(sbVictims);
        sbAllComps = new ArrayList<String>();
        sbAllComps.addAll(sbVictims);
        sbAllComps.addAll(sbTargets);
        
        
        sbTopoEmitRates = new HashMap<String, TreeMap<String, Double> >();
    	sbTopoExecRates = new HashMap<String, TreeMap<String, Double> >();
    	sbCongestionMap = new HashMap<String, HashMap<Component, Double>>();
    	sbParaMap = new HashMap<String, HashMap<String, Integer>>();
    	sourceListMap = new HashMap<String, ArrayList<Component>>();
        
        //initialize ETP Component
        
        //initialize target schedule and statistics
        for(int i=0; i<sbAllComps.size();i++){
        	TopologySchedule targetSchedule = this.sbTopoScheds.get(sbTargets.get(i));
            TopologyStatistics targetStatistics = this.sbTopoStats.get(sbTargets.get(i));  
            HashMap<String, Double> componentEmitRates = new HashMap<String, Double>();
            HashMap<String, Double> componentExecuteRates = new HashMap<String, Double>();
            TreeMap<String, Double> expectedEmitRates = new TreeMap<String, Double>();
            TreeMap<String, Double> expectedExecutedRates = new TreeMap<String, Double>();
            HashMap<String, Integer> parallelism = new HashMap<String, Integer>();
            ArrayList<Component> sourceList = new ArrayList<Component>();
            ETPCalculation.collectRates(targetSchedule, targetStatistics, componentEmitRates, componentExecuteRates, parallelism, expectedEmitRates, expectedExecutedRates, sourceList); 
            this.sbTopoEmitRates.put(targetSchedule.getId(), expectedEmitRates);
            this.sbTopoExecRates.put(targetSchedule.getId(), expectedExecutedRates);
            this.sourceListMap.put(targetSchedule.getId(), sourceList);
            this.sbParaMap.put(targetSchedule.getId(), parallelism);
        }
        
        //sandboxing stage start
        ArrayList<ExecutorPair> pairs = new ArrayList<ExecutorPair>();
        ExecutorPair currPair;
        //RankingStrategy victimStrategy = new ETPFluidPredictionStrategy(victimSchedule, victimStatistics);
        int count = 0;
        while(sbTargets.size()>0 && sbVictims.size()>0 && count<5){
        	currPair = sandbox();
        	pairs.add(currPair);
        }
       
         //ArrayList<ResultComponent> targetComponent = new ArrayList<ResultComponent>();
        return pairs;
    }

	private ExecutorPair sandbox() {
		// TODO Auto-generated method stub
		//ArrayList<ExecutorPair> ret = new ArrayList<ExecutorPair>();
		//find the ranked list of target executor, descending ranked by etp then juice
		ArrayList<ResultComponent> targetCompRank = new ArrayList<ResultComponent>();

		for(int i=0; i<sbTargets.size();i++){
        	//TopologySchedule targetSchedule = this.sbTopoScheds.get(sbTargets.get(i));
            //TopologyStatistics targetStatistics = this.sbTopoStats.get(sbTargets.get(i)); 
			TreeMap<String, Double> expectedEmitRates = this.sbTopoEmitRates.get(sbTargets.get(i));
            TreeMap<String, Double> expectedExecutedRates = this.sbTopoExecRates.get(sbTargets.get(i));
            
			//this.sbTopoEmitRates.put(targetSchedule.getId(), expectedEmitRates);
            //this.sbTopoExecRates.put(targetSchedule.getId(), expectedExecutedRates);
            ETPFluidPredictionStrategy perTopoTargetStrategy = new ETPFluidPredictionStrategy(expectedEmitRates, expectedExecutedRates, source);
            //initialize with exec, emit rate
            ArrayList<ResultComponent> perTopoRankTargetComponents = perTopoTargetStrategy.executorRankDescending();
            targetCompRank.addAll(perTopoRankTargetComponents);
            //this.sbTopoExecRates.put(sbTargets.get(i), new TreeMap<String, Double>(perTopoTargetStrategy.getExpectedExecutedRates()));
            //this.sbTopoEmitRates.put(sbTargets.get(i), new TreeMap<String, Double>(perTopoTargetStrategy.getExpectedEmitRates()));
            //this.sbCongestionMap.put(sbTargets.get(i), new HashMap<Component, Double>(perTopoTargetStrategy.getCongestionMap()));
        }
		Collections.sort(targetCompRank);
		
		//preserving computing statistics from ETP calculation

        
		//find the ranked list of target executor, descending ranked by etp then juice
		ArrayList<ResultComponent> victimCompRank = new ArrayList<ResultComponent>();
		for(int i=0; i<sbVictims.size();i++){
        	TopologySchedule victimSchedule = this.sbTopoScheds.get(sbVictims.get(i));
            TopologyStatistics victimStatistics = this.sbTopoStats.get(sbVictims.get(i));  
            //ETPFluidPredictionStrategy perTopoVictimStrategy = new ETPFluidPredictionStrategy(victimSchedule, victimStatistics);
            ArrayList<ResultComponent> perTopoRankVictimComponents = perTopoVictimStrategy.executorRankAscending();
            victimCompRank.addAll(perTopoRankVictimComponents);
            this.sbTopoExecRates.put(sbVictims.get(i), perTopoVictimStrategy.getExpectedExecutedRates());
            this.sbTopoEmitRates.put(sbVictims.get(i),  perTopoVictimStrategy.getExpectedEmitRates());
        }
		Collections.sort(victimCompRank);
        
		for (ResultComponent victimComponent : victimCompRank) {
            List<ExecutorSummary> victimExecutorDetails = victimComponent.component.getExecutorSummaries();

            for (ResultComponent targetComponent : targetCompRank) {
                List<ExecutorSummary> targetExecutorDetails = targetComponent.component.getExecutorSummaries();

                for (ExecutorSummary victimSummary : victimExecutorDetails) {
                    for (ExecutorSummary targetSummary : targetExecutorDetails) {

                        if (victimSummary.get_host().equals(targetSummary.get_host())) {
                            ExecutorPair ret = new ExecutorPair(targetSummary, victimSummary);
                            //-----------1. Sandboxing new statistics----------------//
                            updateStatistics(targetSummary, targetComponent, victimSummary, victimComponent); //update execution speed and transfer speed 
                            
                            //-----------2. Update ETP Map base on new stats----------------//
                            HashMap<Component, Double> victimETPMap = updateETP(victimComponent); // recalculate ETP
                            HashMap<Component, Double> targetETPMap = updateETP(targetComponent); // recalculate ETP
                            //sort these ETP Maps
                            //victim:
                            ArrayList<ResultComponent> resultComponents = new ArrayList<ResultComponent>();
                            for (Component component: topologyETPMap.keySet()) {
                                resultComponents.add(new ResultComponent(component, topologyETPMap.get(component), targetComponent.topologyID, "ED-JD"));
                            }

                            Collections.sort(resultComponents, Collections.reverseOrder());
                            return resultComponents;
                            
                            //-----------3. Update Juics base on new stats----------------//
                            updateJuice(targetSummary, victimSummary); // recalculate Juice
                        	return ret;
                        }

                    }
                }
            }
        }
		
		
	}

	

	private void updateStatistics(ExecutorSummary targetSummary, ResultComponent targetComponent, ExecutorSummary victimSummary, ResultComponent victimComponent) {
		// TODO Auto-generated method stub
		//resolve target first, target increase parallelism by 1	
		Double increaseRate = increaseParallelism(targetComponent);//Update exec speed and emit speed
		//travserse all its children
		recursiveUpdate(targetComponent.topologyID, targetComponent.component.getId(), increaseRate);		
		//resolve victim next, victim reduce parallelism by 1	
		Double decreaseRate = decreaseParallelism(targetComponent);//Update exec speed and emit speed
		//travserse all its children
		recursiveUpdate(targetComponent.topologyID, targetComponent.component.getId(), decreaseRate);
	}

	private void recursiveUpdate(String topologyID, String compID, Double rate) {
		// TODO Auto-generated method stub
		Component component = this.sbTopoScheds.get(topologyID).getComponents().get(compID);
		for(int i=0; i<component.getChildren().size();i++){
			String childID = component.getChildren().get(i);
			//if child was original congested, leave it;
			if(this.sbCongestionMap.get(topologyID).containsKey(compID)){
				continue;
			}
			else{
				Double currentExecRate = this.sbTopoExecRates.get(topologyID).get(childID);
				Double currentEmitRate = this.sbTopoEmitRates.get(topologyID).get(childID);
				this.sbTopoExecRates.get(topologyID).put(childID, currentExecRate*rate);
				this.sbTopoEmitRates.get(topologyID).put(childID, currentEmitRate*rate);
				Component child = this.sbTopoScheds.get(topologyID).getComponents().get(childID);
				recursiveUpdate(topologyID, childID, rate);
			}
		
			
		}
		
	}

	private Double increaseParallelism(ResultComponent targetComponent) {
		// TODO Auto-generated method stub
		Component target = targetComponent.component;
		int oldpara = target.getParallelism();
		target.setParallelism(oldpara+1);
		//update exec Rate
        Double currentExecRate = this.sbTopoExecRates.get(targetComponent.topologyID).get(targetComponent.component.getId());
        this.sbTopoExecRates.get(targetComponent.topologyID).put(targetComponent.topologyID, currentExecRate*(oldpara+1)/oldpara);
        //update emit Rate
        Double currentEmitRate = this.sbTopoEmitRates.get(targetComponent.topologyID).get(targetComponent.component.getId());
        this.sbTopoExecRates.get(targetComponent.topologyID).put(targetComponent.topologyID, currentEmitRate*(oldpara+1)/oldpara);
        
        return (double) ((oldpara+1)/oldpara);
	}
	
	private Double decreaseParallelism(ResultComponent targetComponent) {
		// TODO Auto-generated method stub
		Component target = targetComponent.component;
		int oldpara = target.getParallelism();
		target.setParallelism(oldpara-1);
		//update exec Rate
        Double currentExecRate = this.sbTopoExecRates.get(targetComponent.topologyID).get(targetComponent.component.getId());
        this.sbTopoExecRates.get(targetComponent.topologyID).put(targetComponent.topologyID, currentExecRate*(oldpara-1)/oldpara);
        //update emit Rate
        Double currentEmitRate = this.sbTopoEmitRates.get(targetComponent.topologyID).get(targetComponent.component.getId());
        this.sbTopoExecRates.get(targetComponent.topologyID).put(targetComponent.topologyID, currentEmitRate*(oldpara-1)/oldpara);
        
        return (double) ((oldpara-1)/oldpara);
	}

	private HashMap<Component, Double> updateETP(ResultComponent targetComponent) {
		// TODO Auto-generated method stub
		//recalculate ETP
		TopologySchedule topologySchedule = this.sbTopoScheds.get(targetComponent.topologyID);
		HashMap<Component, Double> topologyETPMap = new HashMap<Component, Double>();
		ETPCalculation.congestionDetection(topologySchedule, this.sbTopoExecRates.get(targetComponent.topologyID), this.sbTopoEmitRates.get(targetComponent.topologyID), this.sbCongestionMap.get(targetComponent.topologyID));
		TreeMap<String, Double> expectedEmitRates = this.sbTopoEmitRates.get(targetComponent.topologyID);
		
        Double totalThroughput = 0.0;
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                totalThroughput += expectedEmitRates.get(component.getId());
            }
        }

        /*if (totalThroughput == 0.0) {
            LOG.info("Nothing to do as throughput is 0.");
            new TreeMap<>();
        }*/

        HashMap<String, Double> sinksMap = new HashMap<String, Double>();
        for (Component component: topologySchedule.getComponents().values()) {
            if (component.getChildren().size() == 0) {
                Double throughputOfSink = expectedEmitRates.get(component.getId());
                sinksMap.put(component.getId(), throughputOfSink / totalThroughput);
            }
        }

        for (Component component : topologySchedule.getComponents().values()) {
            Double score = ETPCalculation.etpCalculation(topologySchedule, component, sinksMap, this.sbCongestionMap.get(targetComponent.topologyID));
            topologyETPMap.put(component, score);
        }

        return topologyETPMap;
        
	}
	
	private void updateJuice(ExecutorSummary targetSummary, ExecutorSummary victimSummary) {
		// TODO Auto-generated method stub
		
	}

    
}
