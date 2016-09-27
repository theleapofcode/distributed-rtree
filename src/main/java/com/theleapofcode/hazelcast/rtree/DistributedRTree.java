package com.theleapofcode.hazelcast.rtree;

import java.util.Collection;
import java.util.Map;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class DistributedRTree {

	private HazelcastInstance instance = null;
	private final Map<String, RTree<String, Geometry>> rTreesMap;

	public DistributedRTree() {
		Config config = new Config();
		SerializerConfig productSerializer = new SerializerConfig().setTypeClass(RTree.class)
				.setImplementation(new RTreeSerializer());
		config.getSerializationConfig().addSerializerConfig(productSerializer);
		instance = Hazelcast.newHazelcastInstance(config);

		rTreesMap = instance.getMap("RTreesMap");
	}

	public void addRTree(String key, RTree<String, Geometry> rTree) {
		rTreesMap.put(key, rTree);
	}

	public void removeRTee(String key) {
		rTreesMap.remove(key);
	}

	public RTree<String, Geometry> getRTree(String key) {
		return rTreesMap.get(key);
	}

	public Collection<RTree<String, Geometry>> getRTrees() {
		return rTreesMap.values();
	}

	public void shutdown() {
		instance.shutdown();
	}

}
