package heap;

import global.*;
import java.io.*;

class PageDsc implements GlobalConst{


  int    availableSpace; 
  int    recNum;    
  PageId pageId = new PageId();
  private byte [] data ;

  private int offset;
  public static final int size = 12;// size of DataPageInfo

  public PageDsc()
  {  
    data = new byte[size];
    availableSpace = 0;
    recNum =0;
    pageId.pid = INVALID_PAGE;
    offset = 0;
  }

  public PageDsc(Tuple tuple)
       throws InvalidTupleSizeException, IOException
  {   
    if (tuple.getLength()!=size){
      throw new InvalidTupleSizeException(null, "invalid tuple size");
    }

    else{
      data = tuple.returnTupleByteArray();
      offset = tuple.getOffset();
      
      availableSpace = Convert.getIntValue(offset, data);
      recNum = Convert.getIntValue(offset+4, data);
      pageId.pid = Convert.getIntValue(offset+8, data);  
    }
  }
  
  public Tuple convertToTuple()throws IOException{
	    Convert.setIntValue(availableSpace, offset, data);
	    Convert.setIntValue(recNum, offset+4, data);
	    Convert.setIntValue(pageId.pid, offset+8, data);  
	    return new Tuple(data, offset, size);
  }
}