package com.globo.pig.udf;

import java.util.Arrays;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

import org.junit.Test;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

public class CosinesTest {
	
	private static Tuple input;
	
	@BeforeClass
	public static void testSetUp() {
		Tuple t1 = TupleFactory.getInstance().newTuple(Arrays.asList(10001, "tecnologia", 0.7));
		Tuple t2 = TupleFactory.getInstance().newTuple(Arrays.asList(10001, "inovacao", 0.2));
		Tuple t3 = TupleFactory.getInstance().newTuple(Arrays.asList(10001, "internet", 0.5)); 
		DataBag bag1 = BagFactory.getInstance().newDefaultBag(Arrays.asList(t1, t2, t3));
		Tuple t4 = TupleFactory.getInstance().newTuple(Arrays.asList(10003, "infraestrutura", 0.4));
		Tuple t5 = TupleFactory.getInstance().newTuple(Arrays.asList(10003, "internet", 0.3));
		Tuple t6 = TupleFactory.getInstance().newTuple(Arrays.asList(10003, "tecnologia", 0.8));
		DataBag bag2 = BagFactory.getInstance().newDefaultBag(Arrays.asList(t4, t5, t6));
		Tuple pre_input = TupleFactory.getInstance().newTuple(Arrays.asList(bag1,bag2));
		input = TupleFactory.getInstance().newTuple(Arrays.asList(pre_input));
	}

	@Test
	public void test() throws ExecException {
		Cosines cos = new Cosines();
		System.out.println(input.toString());
		assertEquals("cosSim", "0.8521499915989437", cos.exec(input));
	}

}
