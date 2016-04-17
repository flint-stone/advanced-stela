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
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPFluidStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPMatchingStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.JuiceUpdater;
import backtype.storm.scheduler.advancedstela.slo.Observer;

public class MatchingStaticSelector implements Selector {

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
	private HashMap<String, Double> sbJuiceDistMap;
	private HashMap<String, Double> sloMap;
	private Observer observer;
	@Override
	public ArrayList<ExecutorPair> selectPairs(GlobalState globalState, GlobalStatistics globalStatistics, ArrayList<String> targetIDs, ArrayList<String> victimIDs, Observer sloObserver){
        //deep copy globalstatistics and globalstate
        //GlobalState sbState = new GlobalState(globalState.getConfig(), snimbusClient, File advanced_scheduling_log, HashMap<String, TopologySchedule> topologySchedules, HashMap<String, Node> supervisorToNode)
        sbTopoScheds = new HashMap<String, TopologySchedule>();
        sbTopoStats = new HashMap<String, TopologyStatistics>();
        sloMap = new HashMap<String, Double>();
        this.sbTopoScheds.putAll(globalState.getTopologySchedules());
        this.sbTopoStats.putAll(globalStatistics.getTopologyStatistics());
        this.observer = sloObserver;
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
    	sbJuiceDistMap = new HashMap<String, Double>();
        
        //initialize ETP Component
        
        //initialize target schedule and statistics
    	for(int i=0; i<sbAllComps.size();i++){
        	TopologySchedule schedule = this.sbTopoScheds.get(sbAllComps.get(i));
            TopologyStatistics statistics = this.sbTopoStats.get(sbAllComps.get(i));  
            HashMap<String, Double> componentEmitRates = new HashMap<String, Double>();
            HashMap<String, Double> componentExecuteRates = new HashMap<String, Double>();
            TreeMap<String, Double> expectedEmitRates = new TreeMap<String, Double>();
            TreeMap<String, Double> expectedExecutedRates = new TreeMap<String, Double>();
            HashMap<String, Integer> parallelism = new HashMap<String, Integer>();
            ArrayList<Component> sourceList = new ArrayList<Component>();
            HashMap<Component, Double> cm = new HashMap<Component, Double>();
            ETPCalculation.collectRates(schedule, statistics, componentEmitRates, componentExecuteRates, parallelism, expectedEmitRates, expectedExecutedRates, sourceList); 
            ETPCalculation.congestionDetection(schedule, expectedExecutedRates, expectedEmitRates, cm);
            this.sbCongestionMap.put(sbAllComps.get(i), cm);
            this.sbTopoEmitRates.put(schedule.getId(), expectedEmitRates);
            this.sbTopoExecRates.put(schedule.getId(), expectedExecutedRates);
            this.sourceListMap.put(schedule.getId(), sourceList);
            this.sbParaMap.put(schedule.getId(), parallelism);
        }
        
        //initialize juice-slo distance
        
        for (String topo : sbTopoScheds.keySet()){
        	sloMap.put(topo, this.observer.getTopologies().getStelaTopologies().get(topo).getUserSpecifiedSLO());
        	this.sbJuiceDistMap.put(topo, Math.abs(JuiceUpdater.juiceUpadate(sbTopoScheds.get(topo), sbTopoEmitRates.get(topo), sbTopoExecRates.get(topo), sourceListMap.get(topo))-this.observer.getTopologies().getStelaTopologies().get(topo).getUserSpecifiedSLO()));
        }
        
        //sandboxing stage start
        ArrayList<ExecutorPair> pairs = new ArrayList<ExecutorPair>();
        ExecutorPair currPair;
        //RankingStrategy victimStrategy = new ETPFluidPredictionStrategy(victimSchedule, victimStatistics);
        int count = 0;
        while(sbTargets.size()>0 && sbVictims.size()>0 && count<5){
        	currPair = sandbox();
        	pairs.add(currPair);
        	count++;
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
			TreeMap<String, Double> expectedEmitRates = this.sbTopoEmitRates.get(sbTargets.get(i));
            TreeMap<String, Double> expectedExecutedRates = this.sbTopoExecRates.get(sbTargets.get(i));
            TopologySchedule targetSchedule = this.sbTopoScheds.get(sbTargets.get(i));
            ETPMatchingStrategy perTopoTargetStrategy = new ETPMatchingStrategy(expectedEmitRates, expectedExecutedRates, sourceListMap.get(sbTargets.get(i)), targetSchedule, sbJuiceDistMap);
            ArrayList<ResultComponent> perTopoRankTargetComponents = perTopoTargetStrategy.executorRankDescending();
            targetCompRank.addAll(perTopoRankTargetComponents);
        }
		Collections.sort(targetCompRank);
		
		//preserving computing statistics from ETP calculation

        
		//find the ranked list of target executor, descending ranked by etp then juice
		ArrayList<ResultComponent> victimCompRank = new ArrayList<ResultComponent>();
		for(int i=0; i<sbVictims.size();i++){ 
			TreeMap<String, Double> expectedEmitRates = this.sbTopoEmitRates.get(sbTargets.get(i));
            TreeMap<String, Double> expectedExecutedRates = this.sbTopoExecRates.get(sbTargets.get(i));
            TopologySchedule targetSchedule = this.sbTopoScheds.get(sbTargets.get(i));
            ETPFluidStrategy perTopoTargetStrategy = new ETPFluidStrategy(expectedEmitRates, expectedExecutedRates, sourceListMap.get(sbTargets.get(i)), targetSchedule, sbJuiceDistMap);
            ArrayList<ResultComponent> perTopoRankTargetComponents = perTopoTargetStrategy.executorRankDescending();
            ArrayList<ResultComponent> filteredPerTopoRankTargetComponents = filterUncongested(this.sbCongestionMap.get(sbTargets.get(i)),perTopoRankTargetComponents);
            targetCompRank.addAll(filteredPerTopoRankTargetComponents);
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
                            //Nothing needs to be updated, except moving target/victim out of their list
                            //Moving target and victim out of their list and resort them
                            victimCompRank.remove(victimComponent);
                            targetCompRank.remove(targetComponent);
                        	return ret;
                        }

                    }
                }
            }
        }
		
		return null;
		
	}	

	private ArrayList<ResultComponent> filterUncongested(HashMap<Component, Double> congestedMap, ArrayList<ResultComponent> perTopoRankTargetComponents) {
		// TODO Auto-generated method stub
		ArrayList<ResultComponent> ret = new ArrayList<ResultComponent>();
		for(ResultComponent x : perTopoRankTargetComponents){
			for(Component y : congestedMap.keySet()){
				if(x.component.getId().equals(y.getId())){
					ret.add(x);
				}
			}
		}
		return ret;
	}
    
}
