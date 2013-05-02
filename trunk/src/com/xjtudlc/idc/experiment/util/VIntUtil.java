package com.xjtudlc.idc.experiment.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.Constants;

import com.xjtudlc.idc.experiment.test.TestVint;

public class VIntUtil {
	int lastBytePos;
	public byte[] bytes;
	int lastInt;
	
	public void init(){
		lastBytePos = 0;
		bytes = new byte[128];
		lastInt = 0;
	}
	
	public VIntUtil(){
		init();
	}
	public void addInt(int i) {
        int nextInt = i;
        if(i-lastInt>=0)//因为最后一位是词总共出现的次数，所以要加上这个判断
        	i = i - lastInt;
		//writeByte((byte)i);
		if ((lastBytePos + 5) > bytes.length) {
		   // Biggest possible int does not fit.
		   resizeBytes(oversize(lastBytePos + 5, 1));
		 }
		  
		 // See org.apache.lucene.store.IndexOutput.writeVInt()
		 while ((i & ~0x7F) != 0) { // The high bit of the next byte needs to be set.
		    bytes[lastBytePos++] = (byte) ((i & 0x7F) | ~0x7F);
		    i >>>= 7;
		 }
		 bytes[lastBytePos++] = (byte) i; // Last byte, high bit not set.
		      //size++;
		 lastInt = nextInt;
	}

   void resizeBytes(int newSize) {
    if (newSize != bytes.length) {
      byte[] newBytes = new byte[newSize];
      System.arraycopy(bytes, 0, newBytes, 0, lastBytePos);
      bytes = newBytes;
    }
  }
   
   public void done() {
	      resizeBytes(lastBytePos);
   }
	/**
	 * 扩容，算法来自于Lucene
	 * @param minTargetSize
	 * @param bytesPerElement
	 * @return
	 */
	 public  int oversize(int minTargetSize, int bytesPerElement) {

		    if (minTargetSize < 0) {
		      // catch usage that accidentally overflows int
		      throw new IllegalArgumentException("invalid array size " + minTargetSize);
		    }

		    if (minTargetSize == 0) {
		      // wait until at least one element is requested
		      return 0;
		    }

		    // asymptotic exponential growth by 1/8th, favors
		    // spending a bit more CPU to not tie up too much wasted
		    // RAM:
		    int extra = minTargetSize >> 3;

		    if (extra < 3) {
		      // for very small arrays, where constant overhead of
		      // realloc is presumably relatively high, we grow
		      // faster
		      extra = 3;
		    }

		    int newSize = minTargetSize + extra;

		    // add 7 to allow for worst case byte alignment addition below:
		    if (newSize+7 < 0) {
		      // int overflowed -- return max allowed array size
		      return Integer.MAX_VALUE;
		    }

		    if (Constants.JRE_IS_64BIT) {
		      // round up to 8 byte alignment in 64bit env
		      switch(bytesPerElement) {
		      case 4:
		        // round up to multiple of 2
		        return (newSize + 1) & 0x7ffffffe;
		      case 2:
		        // round up to multiple of 4
		        return (newSize + 3) & 0x7ffffffc;
		      case 1:
		        // round up to multiple of 8
		        return (newSize + 7) & 0x7ffffff8;
		      case 8:
		        // no rounding
		      default:
		        // odd (invalid?) size
		        return newSize;
		      }
		    } else {
		      // round up to 4 byte alignment in 64bit env
		      switch(bytesPerElement) {
		      case 2:
		        // round up to multiple of 2
		        return (newSize + 1) & 0x7ffffffe;
		      case 1:
		        // round up to multiple of 4
		        return (newSize + 3) & 0x7ffffffc;
		      case 4:
		      case 8:
		        // no rounding
		      default:
		        // odd (invalid?) size
		        return newSize;
		      }
		    }
		  }
	/**
	 * 按位输出
	 * @param b
	 */
	public void showByte(byte b){
		System.out.println( ""
		        + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)  
		        + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)  
		        + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)  
		        + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1));
	}
	
	public void testHbaseByte(int[] a){
		byte[] out = new byte[a.length * Bytes.SIZEOF_INT];
	    for (int i = 0; i < a.length; ++i) {
	       Bytes.putInt(out, i * Bytes.SIZEOF_INT, a[i]);
	    }
	    for(byte b:out){
	    	showByte(b);
	    }
	}
	
	public static void main(String args[]){
		int[] a = {1,128,129,16384};
		TestVint tv = new TestVint();
		for(int i=0;i<a.length;i++){
			tv.addInt(a[i]);
		}
//		System.out.println(tv.bytes.length);
//		for(int i=0;i<tv.bytes.length;i++){
//			tv.showByte(tv.bytes[i]);
//			//System.out.println(tv.bytes[i]);
//		}
		tv.testHbaseByte(a);
	}

}
