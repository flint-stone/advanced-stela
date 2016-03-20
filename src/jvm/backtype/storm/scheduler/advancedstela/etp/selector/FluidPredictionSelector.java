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
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPFluidPredictionStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy;
import backtype.storm.scheduler.advancedstela.slo.Observer;

public class FluidPredictionSelector implements Selector {

	private HashMap<String, TopologySchedule> sbTopoScheds;
	private HashMap<String, TopologyStatistics> sbTopoStats;
	ArrayList<String> sbTargets;
	ArrayList<String> sbVictims;
	private HashMap<String, TreeMap<String, Double> > sbTopoEmitRates;
	private HashMap<String, TreeMap<String, Double> > sbTopoExecRates;
	private HashMap<String, HashMap<Component, Double>> sbCongestionMap;
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
        
        //sandboxing stage start
        ArrayList<ExecutorPair> currentPair = new ArrayList<ExecutorPair>();
        ExecutorPair currPair;
        //RankingStrategy victimStrategy = new ETPFluidPredictionStrategy(victimSchedule, victimStatistics);
        int count = 0;
        while(sbTargets.size()>0 && sbVictims.size()>0 && count<5){
        	currPair = sandbox();
        }
       
         //ArrayList<ResultComponent> targetComponent = new ArrayList<ResultComponent>();
        
    }

	private ExecutorPair sandbox() {
		// TODO Auto-generated method stub
		//ArrayList<ExecutorPair> ret = new ArrayList<ExecutorPair>();
		//find the ranked list of target executor, descending ranked by etp then juice
		ArrayList<ResultComponent> targetCompRank = new ArrayList<ResultComponent>();

		for(int i=0; i<sbTargets.size();i++){
        	TopologySchedule targetSchedule = this.sbTopoScheds.get(sbTargets.get(i));
            TopologyStatistics targetStatistics = this.sbTopoStats.get(sbTargets.get(i));  
            //ETPFluidPredictionStrategy perTopoTargetStrategy = new ETPFluidPredictionStrategy(targetSchedule, targetStatistics);
            ArrayList<ResultComponent> perTopoRankTargetComponents = perTopoTargetStrategy.executorRankDescending();
            targetCompRank.addAll(perTopoRankTargetComponents);
            this.sbTopoExecRates.put(sbTargets.get(i), new TreeMap<String, Double>(perTopoTargetStrategy.getExpectedExecutedRates()));
            this.sbTopoEmitRates.put(sbTargets.get(i), new TreeMap<String, Double>(perTopoTargetStrategy.getExpectedEmitRates()));
            this.sbCongestionMap.put(sbTargets.get(i), new HashMap<Component, Double>(perTopoTargetStrategy.getCongestionMap()));
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
                            updateStatistics(targetSummary, targetComponent, victimSummary, victimComponent); //update execution speed and transfer speed 
                            updateETP(targetSummary, victimSummary); // recalculate ETP
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
				this.sbTopoEmitRates.get(topologyID).put(childID, currentExecRate*rate);
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

	private void updateETP(ExecutorSummary targetSummary, ExecutorSummary victimSummary) {
		// TODO Auto-generated method stub
		
	}
	
	private void updateJuice(ExecutorSummary targetSummary, ExecutorSummary victimSummary) {
		// TODO Auto-generated method stub
		
	}

    
}
