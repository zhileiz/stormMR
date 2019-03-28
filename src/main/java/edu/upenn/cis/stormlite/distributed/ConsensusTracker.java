package edu.upenn.cis.stormlite.distributed;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator for Consensus
 * 
 * @author ZacharyIves
 *
 */
public class ConsensusTracker {
	AtomicInteger votesForEos = new AtomicInteger(0);
	
	int votesNeeded;
	
	public ConsensusTracker(int votesNeeded) {
		this.votesNeeded = votesNeeded;
	}
	
	/**
	 * Add another vote towards consensus.
	 * @return true == we have enough votes for consensus end-of-stream.
	 *         false == we don't yet have enough votes.
	 */
	public boolean voteForEos() {
		int votes = votesForEos.incrementAndGet();
		
		if (votes >= votesNeeded)
			return true;
		else
			return false;
	}
}
