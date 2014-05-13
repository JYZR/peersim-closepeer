package com.lajv.closepeer;

import com.lajv.NodeWrapper;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

public class ClosePeerObserver implements Control {

	// ========================= Parameters ===============================
	// ====================================================================

	/**
	 * The protocol to operate on.
	 * 
	 * @config
	 */
	private static final String PAR_PROT = "protocol";

	// =========================== Fields =================================
	// ====================================================================

	/**
	 * The name of this observer in the configuration. Initialized by the constructor parameter.
	 */
	private final String name;

	/**
	 * Protocol identifier; obtained from config property {@link #PAR_PROT}.
	 * */
	private final int pid;

	// ==================== Constructor ===================================
	// ====================================================================

	public ClosePeerObserver(String name) {
		this.name = name;
		pid = Configuration.getPid(name + "." + PAR_PROT);
	}

	@Override
	public boolean execute() {
		double avgSum = 0;
		double highestDistance = 0;
		double lowestDistance = 999999999;
		for (int i = 0; i < Network.size(); i++) {
			Node n = Network.get(i);
			ClosePeerProtocol cpp = (ClosePeerProtocol) n.getProtocol(pid);
			double distanceSum = 0;
			for (NodeWrapper peer : cpp.peers) {
				distanceSum += peer.distance;
				if (peer.distance < lowestDistance)
					lowestDistance = peer.distance;
				if (peer.distance > highestDistance)
					highestDistance = peer.distance;
			}
			if (cpp.peers.size() != 0)
				avgSum += distanceSum / cpp.peers.size();
		}
		double avgDistance = avgSum / Network.size();
		System.out.println(name + ": Average distance to close peers: " + avgDistance);
		System.out.println(name + ": Highest distance to a close peer: " + highestDistance);
		System.out.println(name + ": Lowest distance to a close peer: " + lowestDistance);
		return false;
	}
}
