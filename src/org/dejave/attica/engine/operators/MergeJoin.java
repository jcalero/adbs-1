/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
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

import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;
import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.FileUtil;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;

/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 * 
 */
public class MergeJoin extends NestedLoopsJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The names for the temporary files for the two relations */
    private String leftFile;
    private String rightFile;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>(); 
        try {
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        //
        ////////////////////////////////////////////
        leftFile = FileUtil.createTempFileName();
        getStorageManager().createFile(leftFile);
        rightFile = FileUtil.createTempFileName();
        getStorageManager().createFile(rightFile);
        outputFile = FileUtil.createTempFileName();
        getStorageManager().createFile(outputFile);
    } // initTempFiles()

    
    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @Override
    protected void setup() throws EngineException {
        try {
            System.out.println("done");
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////
            
            // store the left input
            Relation leftRel = getInputOperator(LEFT).getOutputRelation();
            RelationIOManager leftMan =
                new RelationIOManager(getStorageManager(), leftRel, leftFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator(LEFT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) leftMan.insertTuple(tuple);
                }
            }
            
            // store the right input
            Relation rightRel = getInputOperator(RIGHT).getOutputRelation();
            RelationIOManager rightMan = 
                new RelationIOManager(getStorageManager(), rightRel, rightFile);
            done = false;
            while (! done) {
                Tuple tuple = getInputOperator(RIGHT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) rightMan.insertTuple(tuple);
                }
            }
            
            
        	Iterator<Tuple> rIt = leftMan.tuples().iterator();
        	Iterator<Tuple> qIt = rightMan.tuples().iterator();
        	Iterator<Tuple> r2It = leftMan.tuples().iterator();
        	Iterator<Tuple> q2It = rightMan.tuples().iterator();
            
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////

            //
            // you may need to uncomment the following lines if you
            // have not already instantiated the manager -- it all
            // depends on how you have implemented the operator
            //
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(),
                                              outputFile);
            
            mergeJoinRelations(leftMan, rightMan);

            // open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store " + 
                                                     "intermediate relations " +
                                                     "to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // setup()
    
    
    @SuppressWarnings("unchecked")
	private void mergeJoinRelations(RelationIOManager leftMan, RelationIOManager rightMan) throws IOException, StorageManagerException {
//    	int ri = 0;
//    	int qi = 0;
//    	int ri2 = 0;
//    	int qi2 = 0;
    	
    	Iterator<Tuple> rIt = leftMan.tuples().iterator();
    	Iterator<Tuple> qIt = rightMan.tuples().iterator();
    	Iterator<Tuple> r2It = leftMan.tuples().iterator();
    	Iterator<Tuple> q2It = rightMan.tuples().iterator();
    	
    	Tuple r = rIt.next();
    	Tuple q = qIt.next();
    	Tuple r2 = r2It.next();
    	Tuple q2 = q2It.next();
    	
    	while (!(r instanceof EndOfStreamTuple) && !(q instanceof EndOfStreamTuple)) {
    		if (r.getValue(leftSlot).compareTo(q.getValue(rightSlot)) > 0) {
    			q = qIt.next();
    			q2 = q2It.next();
    		} else if (r.getValue(leftSlot).compareTo(q.getValue(rightSlot)) < 0) {
    			r = rIt.next();
    			r2 = r2It.next();
    		} else {
    			evalAndInsertTuples(r, q);
    			
    			q2 = q2It.next();
    			while (!(q2 instanceof EndOfStreamTuple) && r.getValue(leftSlot).equals(q2.getValue(rightSlot))) {
    				evalAndInsertTuples(r, q2);
    				q2 = q2It.next();
    			}    			
    			
    			r2 = r2It.next();
    			while (!(r2 instanceof EndOfStreamTuple) && r2.getValue(leftSlot).equals(q.getValue(rightSlot))) {
    				evalAndInsertTuples(r2, q);
    				r2 = r2It.next();
    			}
    			
    			r = rIt.next();
    			q = qIt.next();
    		}
    	}
    }
    
    private void evalAndInsertTuples(Tuple leftTuple, Tuple rightTuple) throws StorageManagerException {
    	PredicateTupleInserter.insertTuples(leftTuple, rightTuple, getPredicate());
    	boolean value = PredicateEvaluator.evaluate(getPredicate());
    	if (value) {
    	    // the predicate is true -- store the new tuple
    	    Tuple newTuple = combineTuples(leftTuple, rightTuple);
    	    outputMan.insertTuple(newTuple);
    	}
    }
    
    /**
     * Cleans up after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    
    @Override
    protected void cleanup() throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete any temporary files
            //
            ////////////////////////////////////////////
            
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()

} // MergeJoin
