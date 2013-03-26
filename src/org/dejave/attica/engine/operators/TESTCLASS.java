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
	private static int[] slots = { 0 };

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
		for (int i = 0; i < p.getNumberOfTuples() - 1; i++) {
			System.out.print(p.retrieveTuple(i).getValues() + " , ");
		}
		System.out.println(p.retrieveTuple(p.getNumberOfTuples() - 1).getValues());
	}
	
    private static ArrayList<Page> sortPages(ArrayList<Page> pages) {
    	for (Page p : pages) {
    		quickSort(p);
    	}
    	return pages;
    }
	
	@SuppressWarnings("rawtypes")
	public static Page createPage(int size, int tupleSize, int minVal, int maxVal) {
		ArrayList<List<Comparable>> tupleVals = new ArrayList<List<Comparable>>();
		for (int i = 0; i < size; i++) {
			ArrayList<Comparable> val = new ArrayList<Comparable>();
			for (int j = 0; j < tupleSize; j++) {
				Random rGen = new Random();
				int randomNum = rGen.nextInt(maxVal - minVal + 1) + minVal;
				val.add(randomNum);
			}
			tupleVals.add(val);
		}

		Page p = new Page(new Relation(), new PageIdentifier("test", 0));
		for (int i = 0; i < tupleVals.size(); i++) {
			p.addTuple(new Tuple(new TupleIdentifier("test", i), tupleVals
					.get(i)));
		}
		
		return p;
	}
	
	@SuppressWarnings("rawtypes")
	public static Page createPageFromArray(int[] array) {
		ArrayList<List<Comparable>> tupleVals = new ArrayList<List<Comparable>>();
		int size = array.length;
		for (int i = 0; i < size; i++) {
			ArrayList<Comparable> val = new ArrayList<Comparable>();
			val.add(array[i]);
			tupleVals.add(val);
		}

		Page p = new Page(new Relation(), new PageIdentifier("test", 0));
		for (int i = 0; i < tupleVals.size(); i++) {
			p.addTuple(new Tuple(new TupleIdentifier("test", i), tupleVals
					.get(i)));
		}
		
		return p;
	}
	
	@SuppressWarnings("unchecked")
	public static ArrayList<Tuple[]> mergePagesOld(ArrayList<Page> pages) {
		
		ArrayList<Tuple[]> output = new ArrayList<Tuple[]>();
		
		// Indices for the relations and groups
		int ri  = 0;
		int si  = 0;
		int gsi = 0;
		
		// The two relations
		Page R = pages.get(0);
		Page S = pages.get(1);
		
		// The initial tuples
		Tuple r  = R.retrieveTuple(ri);
		Tuple s  = S.retrieveTuple(si);
		Tuple gs = S.retrieveTuple(gsi);
		
		// The join attribute index
		int a = slots[0];
		int b = slots[0];
		
		while (!(r instanceof EndOfStreamTuple) && !(s instanceof EndOfStreamTuple)) {
			while (!(r instanceof EndOfStreamTuple) && r.getValue(a).compareTo(gs.getValue(b)) < 0) {
				ri++;
				r = ri >= R.getNumberOfTuples() ? new EndOfStreamTuple() : R.retrieveTuple(ri);
			} 
			while (!(r instanceof EndOfStreamTuple) && !(gs instanceof EndOfStreamTuple) && r.getValue(a).compareTo(gs.getValue(b)) > 0) {
				gsi++;
				gs = gsi >= S.getNumberOfTuples() ? new EndOfStreamTuple() : S.retrieveTuple(gsi);
			} 
			while (!(r instanceof EndOfStreamTuple) && !(gs instanceof EndOfStreamTuple) && r.getValue(a).equals(gs.getValue(b))){
				si = gsi;
				s = si >= S.getNumberOfTuples() ? new EndOfStreamTuple() : S.retrieveTuple(si);
				while (!(s instanceof EndOfStreamTuple) && r.getValue(a).equals(s.getValue(b))) {
					output.add(joinTuples(r, s));
					si++;
					s = si >= S.getNumberOfTuples() ? new EndOfStreamTuple() : S.retrieveTuple(si);
				}
				ri++;
				r = ri >= R.getNumberOfTuples() ? new EndOfStreamTuple() : R.retrieveTuple(ri);
			}
			if (!(gs instanceof EndOfStreamTuple)) {
				gsi = si;
				gs = gsi >= S.getNumberOfTuples() ? new EndOfStreamTuple() : S.retrieveTuple(gsi);
			}
		}
		
		return output;
	}

	@SuppressWarnings("unchecked")
	public static ArrayList<Tuple[]> mergePages(ArrayList<Page> pages) {
		
		ArrayList<Tuple[]> output = new ArrayList<Tuple[]>();
		
		// Indices for the relations
		int ri  = 0;
		int qi  = 0;
		int ri2 = 0;
		int qi2 = 0;
		
		// The two relations
		Page R = pages.get(0);
		Page Q = pages.get(1);
		
		// The initial tuples
		Tuple r  = R.retrieveTuple(ri);
		Tuple q  = Q.retrieveTuple(qi);
		Tuple r2 = R.retrieveTuple(ri2);
		Tuple q2 = Q.retrieveTuple(qi2);
		
		// The join attribute index
		int a = slots[0];
		int b = slots[0];
		
		while (!(r instanceof EndOfStreamTuple) && !(q instanceof EndOfStreamTuple)) {
			if (r.getValue(a).compareTo(q.getValue(b)) > 0) {
				qi++;
				q = qi < Q.getNumberOfTuples() ? Q.retrieveTuple(qi) : new EndOfStreamTuple();
			} else if (r.getValue(a).compareTo(q.getValue(b)) < 0) {
				ri++;
				r = ri < R.getNumberOfTuples() ? R.retrieveTuple(ri) : new EndOfStreamTuple();
			} else {
				output.add(joinTuples(r, q));
				qi2 = qi+1;
				q2 = qi2 < Q.getNumberOfTuples() ? Q.retrieveTuple(qi2) : new EndOfStreamTuple();
				while (!(q2 instanceof EndOfStreamTuple) && r.getValue(a).equals(q2.getValue(b))) {
					output.add(joinTuples(r, q2));
					qi2++;
					q2 = qi2 < Q.getNumberOfTuples() ? Q.retrieveTuple(qi2) : new EndOfStreamTuple();
				}
				
				ri2 = ri+1;
				r2 = ri2 < R.getNumberOfTuples() ? R.retrieveTuple(ri2) : new EndOfStreamTuple();
				while (!(r2 instanceof EndOfStreamTuple) && r2.getValue(a).equals(q.getValue(b))) {
					output.add(joinTuples(r2, q));
					ri2++;
					r2 = ri2 < R.getNumberOfTuples() ? R.retrieveTuple(ri2) : new EndOfStreamTuple();
				}
				
				ri++;
				r = ri < R.getNumberOfTuples() ? R.retrieveTuple(ri) : new EndOfStreamTuple();
				qi++;
				q = qi < Q.getNumberOfTuples() ? Q.retrieveTuple(qi) : new EndOfStreamTuple();
			}
		}
		return output;
	}
	
	public static Tuple[] joinTuples(Tuple left, Tuple right) {
		Tuple[] output = { left, right };
		return output;
	}
	
	public static void printTupleArr(ArrayList<Tuple[]> arr) {
		for (Tuple[] t : arr) {
			System.out.println("[" + t[0].getValues() + ", " + t[1].getValues() + "]");
		}
	}

	public static void main(String[] args) {
//		Page p1 = createPage(5, 1, 0, 4);
//		Page p2 = createPage(5, 1, 0, 4);
		int[] a1 = {1, 2, 3, 2, 1};
		int[] a2 = {1, 2, 3, 2, 1};
		Page p1 = createPageFromArray(a1);
		Page p2 = createPageFromArray(a2);
		ArrayList<Page> ap = new ArrayList<Page>();
		ap.add(p1);
		ap.add(p2);
		// Print input
//		for (Page p : ap) {
//			printArr(p);
//		}
		
		// Sort
		sortPages(ap);
		
		// Print sorted input
		System.out.println("------------------------------------------------");
		for (Page p : ap) {
			printArr(p);
		}
		
		// Merge
		ArrayList<Tuple[]> out = mergePages(ap);
				
		// Print output
		System.out.println("------------------------------------------------");
		printTupleArr(out);
	}
	
	
}
