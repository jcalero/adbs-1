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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;

import org.dejave.attica.storage.FileUtil;

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
            // Read in the input file in batches of B pages
        	///////
            /*Relation rel = getInputOperator().getOutputRelation();
            RelationIOManager inputIOMan =
                new RelationIOManager(sm, rel, inputFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator().getNext();
                System.out.println(FileUtil.getNumberOfPages(inputFile));
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) inputIOMan.insertTuple(tuple);
                }
            }*/
        	long time = System.currentTimeMillis();
            Relation rel = getInputOperator().getOutputRelation();
            RelationIOManager inputIOMan = 
            	new RelationIOManager(sm, rel, inputFile);
            boolean done = false;
            while (!done) {
            	Tuple tuple = getInputOperator().getNext();
            	
            	if (tuple != null) {
            		done = (tuple instanceof EndOfStreamTuple);
            		if (!done) {
            			inputIOMan.insertTuple(tuple);
            			if (FileUtil.getNumberOfPages(inputFile) == buffers) {
            				sortAndStoreBufferedPages(inputIOMan);
            			}
            		}
            	}
            }
            System.out.println((float)(System.currentTimeMillis() - time));
            System.out.println(FileUtil.getNumberOfPages(inputFile));
            
            ////// 
            // The input is now in inputIOMan and can be
            // read page by page, tuple by tuple.
            /////
            
            //////
            // We should now take the input, fetch the B number
            // of pages from it, sort them, and store them in a
            // temporary file.
            /////
//            ArrayList<Page> bufferedPages = new ArrayList<Page>();
//            for (Page p : inputIOMan.pages()) {
//            	bufferedPages.add(p);
//            	if (bufferedPages.size() == buffers) {
////            		sortAndStoreBufferedPages(bufferedPages);
//            	}
//            }
            
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
            
            sm.createFile(outputFile);       
            outputMan = new RelationIOManager(sm, getOutputRelation(),
                                              outputFile);
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
    
    
    private void sortAndStoreBufferedPages(RelationIOManager inputIOManager) throws IOException, StorageManagerException {
    	// Pretend sort...
    	// Sorting... la la lala... DONE Sorted!
    	
    	for (Page p : inputIOManager.pages()) {
    		
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
