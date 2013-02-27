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
    private ArrayList<RelationIOManager> tempIOManagers;
	
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
    
    private int mergeCounter;

    
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
        	long time = System.currentTimeMillis();
        	///////
            // Read in the input to an IO Manager (to a file if needed)
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
            System.out.println(">> Number of pages: " +
            		FileUtil.getNumberOfPages(inputFile));
            ////// 
            // The input is now in inputIOMan and can be
            // read page by page, tuple by tuple.
            /////
            
            //////
            // We should now take the input, fetch the B number
            // of pages from it, sort them, and store them in a
            // temporary file.
            /////
            tempFiles = new ArrayList<String>();
            tempIOManagers = new ArrayList<RelationIOManager>();
            ArrayList<Tuple> bufferedTuples = new ArrayList<Tuple>();
            int pageCount = 0;
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
            
            System.out.println(">> Number of temporary files: " +
            		tempFiles.size());
            System.out.println(">> Ceiling(" +
            		FileUtil.getNumberOfPages(inputFile) +
            		"/" +
            		buffers +
            		") = " +
            		tempFiles.size());
            
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
            /////
            initMergeRun();
            
            System.out.println(">> Sorting took: " +
            		(float)(System.currentTimeMillis() - time)*0.001 +
            		"s");

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
    
//    private void sortPages(ArrayList<Page> pages) {
//    	for (Page p : pages) {
//    		quickSort(p);
//    	}
//    }
    
    private void initMergeRun() throws IOException, StorageManagerException, EngineException {
    	
    	/////////////////////////////////////////
    	// Temporary merge placeholder.
    	// Simply copies the input to the output
    	// Remove when proper merge is implemented
//        for (RelationIOManager man : tempIOManagers) {
//        	for (Page p : man.pages()) {
//        		for (Tuple t : p) {
//        			outputMan.insertTuple(t);
//        		}
//        	}
//        }
//        tempIOManagers.clear();
        /////////////////////////////////////////
        
        /////
        // 
        /////
    	RelationIOManager tempRel = null;
        while (tempIOManagers.size() > mergeCounter) {
	        if (tempIOManagers.size() < buffers - 1) {
	        	tempRel = mergeRun(tempIOManagers.size());
	        } else {
	        	tempRel = mergeRun(buffers - 1);
	        }
        }
        if (tempRel != null) {
	        for (Tuple t : tempRel.tuples()) {
	        	outputMan.insertTuple(t);
	        }
        }
    }
    
    @SuppressWarnings("unchecked")
	private RelationIOManager mergeRun(int fileCount) throws IOException, StorageManagerException, EngineException {
    	// The current index being compared on each
    	// of the lists being merged
    	ArrayList<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
    	ArrayList<Tuple> cachedTuples = new ArrayList<Tuple>();
    	int pageCount = 0;
    	
    	for (int i = 0; i < fileCount; i++) {
    		for( Page p : tempIOManagers.get(i).pages() ) {
	    		Iterator<Tuple> iter = p.iterator();
	    		iterators.add(iter);
	    		cachedTuples.add(iter.next());
	    		pageCount ++;
    		}
    	}
    	
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
	    	for (int i = 0; i < pageCount; i++) {
	    		if (iterators.get(i).hasNext()) {
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
	    	
	    	done = endCount == pageCount;
	    	
	    	if (!done) {
		    	// Add the minumum value to the output manager.
		    	outMan.insertTuple(minTuple);
		    	cachedTuples.set( minIndex, iterators.get( minIndex ).next() );
		    	minTuple = null;
	    	}
    	}
    	
    	mergeCounter += fileCount;
    	
    	return outMan;
    }
    
	private void quickSort(ArrayList<Tuple> tuples) {
		quickAux(tuples, 0, tuples.size() - 1);
	}

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
