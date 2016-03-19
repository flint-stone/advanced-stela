package backtype.storm.scheduler.advancedstela.etp.selector;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.scheduler.advancedstela.etp.GlobalState;
import backtype.storm.scheduler.advancedstela.etp.GlobalStatistics;
import backtype.storm.scheduler.advancedstela.etp.TopologySchedule;
import backtype.storm.scheduler.advancedstela.etp.TopologyStatistics;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPFluidPredictionStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ETPStrategy;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.ExecutorPair;
import backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy.RankingStrategy;
import backtype.storm.scheduler.advancedstela.slo.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FluidPredictionSelector implements Selector {

    @Override
	public ArrayList<ExecutorPair> selectPairs(GlobalState globalState, GlobalStatistics globalStatistics, ArrayList<String> targetIDs, ArrayList<String> victimIDs, Observer sloObserver){
    	
        TopologySchedule victimSchedule = globalState.getTopologySchedules().get(victimIDs.get(0));
       
        TopologyStatistics victimStatistics = globalStatistics.getTopologyStatistics().get(victimIDs.get(0));

        //deep copy globalstatisticscx 
        
        //RankingStrategy victimStrategy = new ETPFluidPredictionStrategy(victimSchedule, victimStatistics);

       
         //ArrayList<ResultComponent> targetComponent = new ArrayList<ResultComponent>();
        for(int i=0; i<targetIDs.size();i++){
        	TopologySchedule targetSchedule = globalState.getTopologySchedules().get(targetIDs.get(i));
            TopologyStatistics targetStatistics = globalStatistics.getTopologyStatistics().get(targetIDs.get(i));  
            RankingStrategy targetStrategy = new ETPFluidPredictionStrategy(targetSchedule, targetStatistics);
            ArrayList<ResultComponent> rankTarget = targetStrategy.executorRankDescending();

        }
        ArrayList<ResultComponent> rankTarget = targetStrategy.executorRankDescending();
        ArrayList<ExecutorPair> ret = new ArrayList<ExecutorPair>();
        
        while(expectedTargetIDs.size()!=0 && expectedVictimIDs.size()!=0){
        	ExecutorPair exchange = fluidSelect(globalState, globalStatistics, expectedTargetIDs, expectedVictimIDs, sloObserver);
        	ret.add(exchange);
        }
        
        return null;
    }

	private ExecutorPair fluidSelect(GlobalState globalState, GlobalStatistics globalStatistics,
			ArrayList<String> expectedTargetIDs, ArrayList<String> expectedVictimIDs, Observer sloObserver) {
				return null;
		// TODO Auto-generated method stub
				//ArrayList<ResultComponent> rankTarget = expectedTargetStrategy.executorRankDescending();
				//ArrayList<ResultComponent> rankVictim = victimStrategy.executorRankAscending();
		       for(String s: expectedVictimIDs.) 
	}
    
    
}
