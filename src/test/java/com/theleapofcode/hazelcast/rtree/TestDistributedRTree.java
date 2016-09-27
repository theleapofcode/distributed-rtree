package com.theleapofcode.hazelcast.rtree;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;

public class TestDistributedRTree {

	private static DistributedRTree drtree;

	@BeforeClass
	public static void startup() {
		drtree = new DistributedRTree();
	}

	@AfterClass
	public static void cleanup() {
		drtree.shutdown();
	}

	@Test
	public void testDisplayLoggedOnUsers() {
		RTree<String, Geometry> rTree = RTree.create();
		rTree = rTree.add("p1", Geometries.point(10, 20));

		drtree.addRTree("rtree1", rTree);

		RTree<String, Geometry> rTree2 = drtree.getRTree("rtree1");

		Assert.assertEquals(rTree2.asString(), rTree.asString());
	}

}
