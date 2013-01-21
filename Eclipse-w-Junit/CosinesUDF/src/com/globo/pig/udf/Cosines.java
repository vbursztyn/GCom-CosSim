package com.globo.pig.udf;

import java.lang.String;
import java.lang.Double;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class Cosines extends EvalFunc<String> {
	
	//INPUT:
	//({ (id1, p1, s1), (id1, p2, s2), (id1, p3, s3) }, id2, { (id2, p1, s1), (id2, p2, s2), (id2, p3, s3) })  
	public String exec(Tuple cosInput) throws ExecException {
		if (cosInput == null || cosInput.size() == 0)
			return null;
		
		Tuple cosPair = (Tuple) cosInput.get(0);
		
		DataBag tupleVecA = (DataBag) cosPair.get(0); //vec_A stores { (id1, p1, s1), (id1, p2, s2), (id1, p3, s3) } 
		DataBag tupleVecB = (DataBag) cosPair.get(1); //vec_B stores { (id2, p1, s1), (id2, p2, s2), (id2, p3, s3) }
		
		double intersection = 0.0; //Accumulating the weight of the intersection, from the times "p<n>" appear in both vectors.
		double[] valuesVecA = new double[3];
		double[] valuesVecB = new double[3];
		int i = 0, j;
			
		//vecA iterates over (id1, p<n>, s<n>)
		for (Tuple vecA : tupleVecA) {
			String pA = vecA.get(1).toString();
			valuesVecA[i] = Double.valueOf(vecA.get(2).toString());
			i++; j = 0;
			
			//vecB iterates over (id2, p<n>, s<n>)
			for (Tuple vecB : tupleVecB) {
				String pB = vecB.get(1).toString();
				valuesVecB[j] = Double.valueOf(vecB.get(2).toString()); j++;
				
				if (pB.equals(pA) == true) { //If current "p<n>" from vec_A matches a "p<n>" from vec_B
					intersection += Double.valueOf(vecA.get(2).toString()) * Double.valueOf(vecB.get(2).toString());
				}
			}
				
		}
		
		double cosSim = intersection / (this.norm3(valuesVecA) * this.norm3(valuesVecB));  
		
		return Double.toString(cosSim);

	}
	
	private double norm3 (double[] valuesVec) {
		double sumVec = 0.0; 
		for (int i = 0; i < 3; i++) {
			sumVec += valuesVec[i] * valuesVec[i]; 
		}
		return Math.sqrt(sumVec);
	}
}