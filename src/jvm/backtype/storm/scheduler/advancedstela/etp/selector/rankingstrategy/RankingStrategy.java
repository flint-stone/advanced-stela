package backtype.storm.scheduler.advancedstela.etp.selector.rankingstrategy;

import java.util.ArrayList;

import backtype.storm.scheduler.advancedstela.etp.selector.ResultComponent;

public interface RankingStrategy {

	ArrayList<ResultComponent> executorRankDescending();

	ArrayList<ResultComponent> executorRankAscending();

}