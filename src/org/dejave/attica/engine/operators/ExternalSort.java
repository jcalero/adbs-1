/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.FileUtil;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;

/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The name of the temporary file for the input */
    private String inputFile;
    
    /** The temporary files used for the intermediate steps */
    private ArrayList<String> tempFiles;
    
    /** The temporary IO Managers used for the intermediate steps */
    private ArrayDeque<RelationIOManager> tempIOManagers;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the sort keys. */
    private int [] slots;
	
    /** Number of buffers (i.e., buffer pool pages and 
     * output files). */
    private int buffers;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private List<Tuple> returnList;

    
    /**
     * Constructs a new external sort operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the sort keys.
     * @param buffers the number of buffers (i.e., run files) to be
     * used for the sort. (maximum number of pages to use in sorting at the time)
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized.
     */
    public ExternalSort(Operator operator, StorageManager sm,
                        int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
        try {
            // create the temporary output files
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate external sort",
                                      sme);
        }
    } // ExternalSort()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        // in the event of an error
        //
        // for the time being, the only file we
        // know of is the output file
        //
        ////////////////////////////////////////////
    	inputFile = FileUtil.createTempFileName();
    	sm.createFile(inputFile);
    	
        outputFile = FileUtil.createTempFileName();
    } // initTempFiles()

    
    /**
     * Sets up this external sort operator.
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     */
    public void setup() throws EngineException {
        returnList = new ArrayList<Tuple>();
        try {
            ////////////////////////////////////////////
            //
            // this is a blocking operator -- store the input
            // in a temporary file and sort the file
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////
        	
        	///////
            // Read in the input to an IO Manager (and output to file if needed)
        	///////
            Relation rel = getInputOperator().getOutputRelation();
            RelationIOManager inputIOMan =
                new RelationIOManager(sm, rel, inputFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator().getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) inputIOMan.insertTuple(tuple);
                }
            }
            ////// 
            // The input is now in inputIOMan and can be
            // read page by page or tuple by tuple.
            /////
            
            //////
            // We should now take the input, fetch the B number
            // of pages from it, sort them, and store them in a
            // temporary file. Repeat until input is exhausted.
            /////
            
            // A record of the temporary files, stored as reference
            // for later cleaning up.
            tempFiles = new ArrayList<String>();
            
            // A record of the temporary IO Managers, one for each temporary file.
            // This record is our main record when doing the merging.
            // 
            // This is an ArrayDeque to allow pushing and pulling from both the
            // front and end of the list. Reasons explained in the merge function.
            tempIOManagers = new ArrayDeque<RelationIOManager>();
            
            // A record of tuples currently being read from the input.
            // These tuples are the ones passed to the sorting and output
            // function once B pages have been read.
            ArrayList<Tuple> bufferedTuples = new ArrayList<Tuple>();
            
            // Counter for how many pages have been read so far.
            int pageCount = 0;
            
            ///////
            // Read the pages from the input, add the tuples to 
            // bufferedTuples, once a page has been read, increment
            // pagecounter. If pagecounter is the same as buffers,
            // run the sorting & output function.
            // Clear the bufferedTuples and reset, repeat for the
            // remaining pages in the input.
            //
            // Sorting is done as in-place quick-sort with the 
            // last element as pivot. This simplifies the 
            // implementation although it might cause
            // O(n^2) performance on reverse sorted data. 
            ///////
            for (Page p : inputIOMan.pages()) {
            	for (Tuple t : p) {
            		bufferedTuples.add(t);
            	}
            	pageCount++;
            	if (pageCount == buffers) {
            		initTempFileRun(bufferedTuples);
            		bufferedTuples.clear();
            		pageCount = 0;
            	}
            }
            
            //////
            // Make sure the final set of pages smaller than 
            // the buffer size also are added to a file.
            //////
            if (pageCount > 0) {
            	initTempFileRun(bufferedTuples);
            	bufferedTuples.clear();
            	pageCount = 0;
            }
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
            
            sm.createFile(outputFile);       
            outputMan = new RelationIOManager(sm, getOutputRelation(),
                                              outputFile);
            
            
            /////
            // We now have a set of temporary files (X/B to be
            // precise, where X is the number of pages in the
            // input), which are all sorted. We also have defined
            // the output IO manager and can now start merging.
            // 
            // Our sorted files reside in the class member
            // tempIOManager and the merge function will output
            // its result to the class member outputMan, hence
            // no arguments or return types are needed for the
            // merge function.
            /////
            initMergeRun();

            outputTuples = outputMan.tuples().iterator();
            
            
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not store intermediate relations"
                                      + "to files.", sme);
        }
        catch (IOException ioe) {
        	throw new EngineException("Could not create page/tuple iterators.", ioe);
        }
    } // setup()
    
    /**
     * The sorting and output run of the temporary chunks given by B pages from input.
     * Uses in-place quicksort with the final element as pivot for sorting.
     * 
     * @param tuples - The data to sort.
     * @throws IOException
     * @throws StorageManagerException
     * @throws EngineException
     */
    private void initTempFileRun(ArrayList<Tuple> tuples) throws IOException, StorageManagerException, EngineException {
    	// Sort the tuples
    	quickSort(tuples);
    	
    	// Create the temporary file to output to
    	String tempFile = FileUtil.createTempFileName();
    	sm.createFile(tempFile);
    	tempFiles.add(tempFile);
    	
    	// Create the IO Manager for the temporary file
    	RelationIOManager tempRel = new RelationIOManager(sm, getOutputRelation(), tempFile);
    	tempIOManagers.add(tempRel);
    	
    	// Insert the sorted tuples into the IO manager so that it writes the output
    	// file
    	for (Tuple t : tuples) {
        	tempRel.insertTuple(t);
    	}
    }
    
    /**
     * The merge run "runner" that keeps track of how many merge runs to call and
     * what to call them on.
     * 
     * @throws IOException
     * @throws StorageManagerException
     * @throws EngineException
     */
    private void initMergeRun() throws IOException, StorageManagerException, EngineException {
        // This is the relation that will contain the intermediate
    	// merged output and finally the entire merged output once all
    	// merge runs have finished.
    	RelationIOManager tempRel = null;
    	
    	// Loop over the files (RelationIOManagers) in tempIOManagers.
    	// Or more precisely, as long as there is more than one temporary
    	// file do a merge run.
    	// 
    	// tempIOManagers is updated at each merge run to contain only the
    	// current run of files. In other words, discards the files already
    	// merged and adds the new merged files.
        while (tempIOManagers.size() > 1) {
	        if (tempIOManagers.size() < buffers - 1) {
	        	// Case for when there are less files than buffers to merge,
	        	// in which case all the remaining files will be merged.
	        	// In other words, the final merge run.
	        	tempRel = mergeRun(tempIOManagers.size());
	        } else {
	        	tempRel = mergeRun(buffers - 1);
	        }
	        
	        // Add the new merged file from the last run to the
	        // end of the queue.	     
	        tempIOManagers.addLast( tempRel );
        }
        
        // Once all files have been merged, the lone file remaining
        // in tempIOManagers will be streamed to the outputManager
        // for the final output of this operator.
        for (Tuple t : tempIOManagers.getLast().tuples()) {
        	outputMan.insertTuple(t);
        }
    }
    
    /**
     * The intermediate merge runs of the temporary files.
     *  
     * @param fileCount - The number of files from tempIOManager to merge.
     * Always grabs the number of files from the top of the queue.
     * @return
     * @throws IOException
     * @throws StorageManagerException
     * @throws EngineException
     */
    @SuppressWarnings("unchecked")
	private RelationIOManager mergeRun(int fileCount) throws IOException, StorageManagerException, EngineException {
    	// The current index being compared on each
    	// of the lists being merged
    	ArrayList<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
    	
    	// The tuples fetched for comparison.
    	ArrayList<Tuple> cachedTuples = new ArrayList<Tuple>();
    	
    	// Fetches the first tuples from each of the files in
    	// tempIOMananger. Stores the tuple iterators for the files in
    	// iterators and removes them from the tempIOManager queue.
    	for (int i = 0; i < fileCount; i++) {
    		Iterator<Tuple> iter = tempIOManagers.pollFirst().tuples().iterator();
    		iterators.add(iter);
    		cachedTuples.add(iter.next());
    	}
    	
    	// Used for merging.
    	Tuple minTuple = null;
    	int minIndex = -1;
    	boolean done = false;
    	
    	// Create temporary file to be used as output
    	String tempFile = FileUtil.createTempFileName();
    	sm.createFile(tempFile);
    	tempFiles.add(tempFile);
    	RelationIOManager outMan = new RelationIOManager(sm, getOutputRelation(), tempFile);
    	
    	while (!done) {
    		// Find the minimum value of the next tuples in
    		// each file being checked.
    		int endCount = 0;
	    	for (int i = 0; i < fileCount; i++) {
	    		if ( cachedTuples.get( i ) != null ) {
	    			Tuple currentTuple = cachedTuples.get(i);
		    		if (minTuple == null) {
		    			minTuple = currentTuple;
		    			minIndex = i;
		    		} else {
		    			int slotsIndex = findSlotsIndex(currentTuple, minTuple, 0);		    			
			    		if (currentTuple.getValue(slots[slotsIndex])
			    				.compareTo(minTuple.getValue(slots[slotsIndex])) < 0) {
			    			minTuple = currentTuple;
			    			minIndex = i;
			    		}
		    		}
	    		} else {
	    			endCount++;
	    		}
	    	}
	    	
	    	// All files have been traversed and the merging is done.
	    	done = endCount == fileCount;
	    	
	    	if (!done) {
		    	// Add the minumum value to the output manager.
		    	outMan.insertTuple(minTuple);
		    	if( iterators.get( minIndex ).hasNext() )
		    		cachedTuples.set( minIndex, iterators.get( minIndex ).next() );
		    	else
		    		cachedTuples.set( minIndex, null );
		    	minTuple = null;
	    	}
    	}
    	
    	return outMan;
    }
    
    /**
     * Quicksort initialiser
     * @param tuples - The data to sort.
     */
	private void quickSort(ArrayList<Tuple> tuples) {
		quickAux(tuples, 0, tuples.size() - 1);
	}
	
	/**
	 * The main inplace quicksort method.
	 * 
	 * @param tuples - The data to sort
	 * @param start - The start index
	 * @param end - The end index
	 */
	@SuppressWarnings("unchecked")
	private void quickAux(ArrayList<Tuple> tuples, int start, int end) {
		if (start < end) {
			Tuple pivot = tuples.get(end);
			int i = start;
			int j = end;
			
			while (i != j) {
				int slotIndex = findSlotsIndex(tuples.get(i), pivot, 0);
				if (tuples.get(i).getValue(slots[slotIndex])
						.compareTo(pivot.getValue(slots[slotIndex])) < 0) {
					i = i + 1;
				} else {
					tuples.set(j, tuples.get(i));
					tuples.set(i, tuples.get(j - 1));
					j = j - 1;
				}
			}
			tuples.set(j, pivot);
			quickAux(tuples, start, j - 1);
			quickAux(tuples, j + 1, end);
		}
	}

	/**
	 * This method is used for identifying what sort key to sort
	 * by. 
	 * 
	 * @param tuple - The currently read tuple
	 * @param otherTuple - The tuple to compare to
	 * @param prevIndex - Recursive index.
	 * @return - The sort key index.
	 */
	@SuppressWarnings({ "unchecked" })
	private int findSlotsIndex(Tuple tuple, Tuple otherTuple,
			int prevIndex) {
		if (tuple.getValue(slots[prevIndex]).compareTo(
				otherTuple.getValue(slots[prevIndex])) == 0) {
			if (prevIndex + 1 == slots.length) {
				return prevIndex;
			} else {
				return findSlotsIndex(tuple, otherTuple, ++prevIndex);
			}
		} else {
			return prevIndex;
		}
	}

    
    /**
     * Cleanup after the sort.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    public void cleanup () throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete the intermediate
            // files after sorting is done
            //
            ////////////////////////////////////////////
            
        	sm.deleteFile(inputFile);
        	
        	for (String file : tempFiles) {
        		sm.deleteFile(file);
        	}
        	
            ////////////////////////////////////////////
            //
            // right now, only the output file is 
            // deleted
            //
            ////////////////////////////////////////////
            sm.deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output.", sme);
        }
    } // cleanup()

    
    /**
     * The inner method to retrieve tuples.
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */    
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples " +
                                      "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Operator class abstract interface -- never called.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

} // ExternalSort
