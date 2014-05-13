package com.lajv.closepeer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import com.lajv.NodeWrapper;
import com.lajv.cyclon.CyclonProtocol;
import com.lajv.vivaldi.VivaldiCoordinate;
import com.lajv.vivaldi.VivaldiProtocol;

import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;

public class ClosePeerProtocol implements CDProtocol {

	// ========================= Parameters ===============================
	// ====================================================================

	/**
	 * The overlay protocol which can be used to fetch peers from
	 * 
	 * @config
	 */
	private static final String PAR_CYCLON_PROT = "cyclon_prot";

	/**
	 * The Vivaldi protocol which holds the coordinate
	 * 
	 * @config
	 */
	private static final String PAR_VIVALDI_PROT = "vivaldi_prot";

	/**
	 * The overlay protocol which can be used to fetch peers from
	 * 
	 * @config
	 */
	private static final String PAR_MAX_SIZE = "max_size";

	// =========================== Fields =================================
	// ====================================================================

	protected List<NodeWrapper> peers;

	protected VivaldiCoordinate coord;

	public static int max_size;
	public static final int default_max_size = 10;

	private String prefix;

	// ==================== Constructor ===================================
	// ====================================================================

	public ClosePeerProtocol(String prefix) {
		this.prefix = prefix;

		peers = new ArrayList<NodeWrapper>();

		// coord can not yet be set

		max_size = Configuration.getInt(prefix + "." + PAR_MAX_SIZE, default_max_size);
	}

	// ====================== Methods =====================================
	// ====================================================================

	@Override
	public void nextCycle(Node node, int protocolID) {
		// Set coordinate in the first cycle
		int vivaldiProtID = Configuration.getPid(prefix + "." + PAR_VIVALDI_PROT);
		if (coord == null) {
			VivaldiProtocol vivaldiProt = (VivaldiProtocol) node.getProtocol(vivaldiProtID);
			coord = vivaldiProt.getCoord();
		}

		// Merge in new peers from Cyclon
		int cyclonProtID = Configuration.getPid(prefix + "." + PAR_CYCLON_PROT);
		CyclonProtocol cyclonProt = (CyclonProtocol) node.getProtocol(cyclonProtID);
		mergePeers(node, cyclonProt.getCache());

		if (peers.size() == 0)
			return;

		// Receive close peers from a random neighbour
		int i = CommonState.r.nextInt(peers.size());
		NodeWrapper peer = peers.get(i);
		ClosePeerProtocol otherClosePeerProt = (ClosePeerProtocol) peer.node
				.getProtocol(protocolID);
		List<NodeWrapper> receivedClosePeers = otherClosePeerProt.findClosePeers(coord);
		mergePeers(node, receivedClosePeers);
		
		// Update coordinate for the other peer
		VivaldiProtocol otherVivaldiProt = (VivaldiProtocol) node.getProtocol(vivaldiProtID);
		peer.coord.update(otherVivaldiProt.getCoord());
	}

	/**
	 * @param otherCoord
	 *            The coordinate of the requesting node
	 * @return A list of NodeWrappers which are closer to the requesting node then the responding
	 *         node itself
	 */
	private List<NodeWrapper> findClosePeers(VivaldiCoordinate otherCoord) {
		double nbDistance = otherCoord.distance(coord);
		List<NodeWrapper> closePeers = new LinkedList<NodeWrapper>();
		for (int i = 0; i < peers.size(); i++) {
			NodeWrapper p = peers.get(i);
			double distance = otherCoord.distance(p.coord);
			if (distance < nbDistance) {
				NodeWrapper copy = p.closePeerCopy();
				copy.distance = distance;
				closePeers.add(copy);
			}
		}
		return closePeers;
	}

	public void mergePeers(Node node, List<NodeWrapper> newPeers) {
		addNewPeers: for (NodeWrapper newPeer : newPeers) {
			if (newPeer.node == node)
				continue;
			for (NodeWrapper peer : peers) {
				if (newPeer.node == peer.node) {
					peer.coord = newPeer.coord;
					peer.distance = coord.distance(newPeer.coord);
					continue addNewPeers;
				}
			}
			newPeer.distance = coord.distance(newPeer.coord);
			peers.add(newPeer);
		}
		Collections.sort(peers, new DistanceComparator());
		while (peers.size() > max_size) {
			peers.remove(peers.size() - 1);
		}
	}

	@Override
	public Object clone() {
		ClosePeerProtocol cpp = null;
		try {
			cpp = (ClosePeerProtocol) super.clone();
		} catch (CloneNotSupportedException e) {
			// never happens
		}
		cpp.peers = new ArrayList<NodeWrapper>();
		return cpp;
	}

	class DistanceComparator implements Comparator<NodeWrapper> {
		@Override
		public int compare(NodeWrapper cp1, NodeWrapper cp2) {
			return (int) Math.ceil(cp1.distance - cp2.distance);
		}
	}
}
