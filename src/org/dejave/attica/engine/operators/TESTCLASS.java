package org.dejave.attica.engine.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.PageIdentifier;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIdentifier;

public class TESTCLASS {
	private static int[] slots = { 2, 1, 0 };

	public static void quickSort(Page p) {
		quickAux(p, 0, p.getNumberOfTuples() - 1);
	}

	@SuppressWarnings("unchecked")
	private static void quickAux(Page p, int start, int end) {
		if (start < end) {
			Tuple pivot = p.retrieveTuple(end);
			int i = start;
			int j = end;
			
			while (i != j) {
				int slotIndex = findSlotsIndex(p.retrieveTuple(i), pivot, 0);
				if (p.retrieveTuple(i).getValue(slots[slotIndex])
						.compareTo(pivot.getValue(slots[slotIndex])) < 0) {
					i = i + 1;
				} else {
					p.setTuple(j, p.retrieveTuple(i));
					p.setTuple(i, p.retrieveTuple(j - 1));
					j = j - 1;
				}
			}
			p.setTuple(j, pivot);
			quickAux(p, start, j - 1);
			quickAux(p, j + 1, end);
		}
	}

	@SuppressWarnings({ "unchecked" })
	private static int findSlotsIndex(Tuple tuple, Tuple pivotTuple,
			int prevIndex) {
		if (tuple.getValue(slots[prevIndex]).compareTo(
				pivotTuple.getValue(slots[prevIndex])) == 0) {
			if (prevIndex + 1 == slots.length) {
				return prevIndex;
			} else {
				return findSlotsIndex(tuple, pivotTuple, ++prevIndex);
			}
		} else {
			return prevIndex;
		}
	}

	public static void printArr(Page p) {
		for (int i = 0; i < p.getNumberOfTuples(); i++) {
			System.out.print(p.retrieveTuple(i).getValues() + " || ");
		}
		System.out.println("");
	}
	
    private static ArrayList<Page> sortPages(ArrayList<Page> pages) {
    	for (Page p : pages) {
    		quickSort(p);
    	}
    	return pages;
    }

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		int tupleSize = 10;
		ArrayList<List<Comparable>> tupleVals = new ArrayList<List<Comparable>>();
		for (int i = 0; i < tupleSize; i++) {
			ArrayList<Comparable> val = new ArrayList<Comparable>();
			for (int j = 0; j < slots.length; j++) {
				Random rGen = new Random();
				int max = 4;
				int min = 0;
				int randomNum = rGen.nextInt(max - min + 1) + min;
				val.add(randomNum);
			}
			tupleVals.add(val);
		}

		Page p = new Page(new Relation(), new PageIdentifier("test", 0));
		for (int i = 0; i < tupleVals.size(); i++) {
			p.addTuple(new Tuple(new TupleIdentifier("test", i), tupleVals
					.get(i)));
		}
		printArr(p);
		ArrayList<Page> ap = new ArrayList<Page>();
		ap.add(p);
		sortPages(ap);
		printArr(p);
	}
}
