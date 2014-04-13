package heap;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

import java.io.IOException;

import diskmgr.Page;


public class Heapfile implements  GlobalConst {

	PageId headerId; // pageId of header
	private String fileName;

	public Heapfile(String name) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException {

		fileName = name;
//		fileType = ORDINARY;

		// case 1: If the file is new and the header page must be initialized
		// case 2: the file already exists then fetch the header page into the buffer pool

		headerId = get_file_entry(fileName);

		if (headerId == null) { // file does not exist, create one
			Page page = new Page();
			headerId = newPage(page, 1);
			add_file_entry(fileName, headerId);

			HFPage firstDirPage = new HFPage();
			firstDirPage.init(headerId, page);

			// next and prev point to invalid page
			PageId pageId = new PageId(INVALID_PAGE);
			firstDirPage.setNextPage(pageId);
			firstDirPage.setPrevPage(pageId);

			unpinPage(headerId, true);
		}

	}

	
	
	public int getRecCnt() throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFBufMgrException, IOException{
		
		int recNum = 0;
		PageId dirPageId = new PageId(headerId.pid);
		HFPage dirPage = new HFPage();
		PageId nextDirPageId = new PageId(0);
		RID dirRecId = new RID();

		while (dirPageId.pid != INVALID_PAGE) {
			pinPage(dirPageId, dirPage, false);

			Tuple pageDisTuple;
			PageDsc pageDsc;
			for (dirRecId = dirPage.firstRecord(); dirRecId != null;dirRecId = dirPage.nextRecord(dirRecId)) {
				pageDisTuple = dirPage.getRecord(dirRecId);
				pageDsc = new PageDsc(pageDisTuple);

				recNum += pageDsc.recNum;
			}
			nextDirPageId = dirPage.getNextPage();
			unpinPage(dirPageId, false);
			dirPageId.pid = nextDirPageId.pid;
		}
		return recNum;
	}
	
	public RID insertRecord(byte[] recPtr) throws InvalidSlotNumberException,InvalidTupleSizeException,
	 HFException,HFBufMgrException, HFDiskMgrException, IOException {

		HFPage dirPage = new HFPage();
		PageId dirPageId = new PageId(headerId.pid);
		RID dirRecId = new RID();
		HFPage dataPage = new HFPage();

		PageId nextDirPageId = new PageId();

		pinPage(dirPageId, dirPage, false); // pin first dir. page

		Tuple pageDscTuple;
		PageDsc pageDsc = new PageDsc();

		boolean found = false;
		while (found == false) {
			for (dirRecId = dirPage.firstRecord(); dirRecId != null; dirRecId = dirPage
					.nextRecord(dirRecId)) {

				pageDscTuple = dirPage.getRecord(dirRecId); // get current record
				pageDsc = new PageDsc(pageDscTuple);

				if (recPtr.length <= pageDsc.availableSpace) {
					found = true;
					break;
				}
			} // current directory page ended or space found

			if (found == false) {
				// there is no dataPageRecord on the current directory page
				// whose corresponding dataPage has enough space free

				if (dirPage.available_space() >= pageDsc.size) {
					// there is enough space on the current directory page to
					// add a new dataPageRecord

					dataPage = newDataPage(pageDsc); // dpinfo is sent by reference
					pageDscTuple = pageDsc.convertToTuple();
					byte[] tmpData = pageDscTuple.getTupleByteArray();
					dirRecId = dirPage.insertRecord(tmpData);

					found = true;

				} else { // look at the next directory page or create it.
					nextDirPageId = dirPage.getNextPage();

					if (nextDirPageId.pid != INVALID_PAGE) {// there is another
															// directory page
						unpinPage(dirPageId, false);
						dirPageId.pid = nextDirPageId.pid;
						pinPage(dirPageId, dirPage, false);// currentDirPage is sent to the buffer as the nextDirPage

					} else { // create a new directory page
						newDirPage(dirPage, dirPageId);
					}
				}

			} else {// funod == true, we have found a dataPage with enough space
				pinPage(pageDsc.pageId, dataPage, false); // pin the dataPage we have found
			}
		}

		RID rid;
		rid = dataPage.insertRecord(recPtr);

		pageDsc.recNum++;
		pageDsc.availableSpace = dataPage.available_space();

		// update dirPage
		pageDscTuple = dirPage.returnRecord(dirRecId);
		
		PageDsc DPInfoTemp = new PageDsc(pageDscTuple);
		DPInfoTemp.availableSpace = pageDsc.availableSpace;
		DPInfoTemp.recNum = pageDsc.recNum;
		DPInfoTemp.pageId.pid = pageDsc.pageId.pid;
		pageDscTuple = DPInfoTemp.convertToTuple();

		unpinPage(dirPageId, true); // unpin dirPage
		unpinPage(pageDsc.pageId, true); // unpin dataPage

		return rid;
	}

	public boolean deleteRecord(RID rid) throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFBufMgrException, Exception{
		
		HFPage dirPage = new HFPage();
		PageId dirPageId = new PageId();
		HFPage dataPage = new HFPage();
		PageId dataPageId = new PageId();
		RID dirRecId = new RID();

		boolean found = findDataPage(rid, dirPageId, dirPage,dataPageId, dataPage, dirRecId);
		if (found != true)return false; // record not found
		
		Tuple pageDisTuple = dirPage.returnRecord(dirRecId);
		PageDsc pageDis = new PageDsc(pageDisTuple);

		// delete the record on the datapage
		dataPage.deleteRecord(rid);
		pageDis.recNum--;
		
		pageDisTuple = dirPage.returnRecord(dirRecId);
		pageDisTuple = pageDis.convertToTuple();
		
		if (pageDis.recNum >= 1) {// page still contains records

			pageDis.availableSpace = dataPage.available_space();
			pageDisTuple = pageDis.convertToTuple();
			
			unpinPage(dataPageId, true);
			unpinPage(dirPageId, true);

		} else {
			// delete dataPage
			unpinPage(dataPageId, false);
			freePage(dataPageId);
			
			// delete pageInfo from dirPage
			dirPage.deleteRecord(dirRecId);

			dirRecId = dirPage.firstRecord();
			PageId pageId = dirPage.getPrevPage();
			
			if ((dirRecId == null) && (pageId.pid != INVALID_PAGE)) {
				// no records in this directory and && not the first dirPage

				// set next pointer of prevPage
				HFPage prevDirPage = new HFPage();
				pinPage(pageId, prevDirPage, false);
				prevDirPage.setNextPage(new PageId(INVALID_PAGE));
				pageId = dirPage.getPrevPage();
				unpinPage(pageId, true);
				
				unpinPage(dirPageId, false);
				freePage(dirPageId);

			} else {
				unpinPage(dirPageId, true);
			}
		}
		return true;
	}

	public boolean updateRecord(RID rid, Tuple newtuple)
			throws InvalidSlotNumberException, InvalidUpdateException,
			InvalidTupleSizeException,HFBufMgrException, Exception {
		
		HFPage dirPage = new HFPage();
		PageId dirPageId = new PageId();
		HFPage dataPage = new HFPage();
		PageId dataPageId = new PageId();
		RID dirRecId = new RID();

		boolean found = findDataPage(rid, dirPageId, dirPage,dataPageId, dataPage, dirRecId);
		if (found != true) return false; // record not found
			Tuple dataTuple = dataPage.returnRecord(rid);

		if (newtuple.getLength() != dataTuple.getLength()) {
			unpinPage(dataPageId, false);
			unpinPage(dirPageId, false);
			throw new InvalidUpdateException(null, "invalid record update");
		}
		dataTuple.tupleCopy(newtuple);
		unpinPage(dataPageId, true);
		unpinPage(dirPageId, false);

		return true;
	}

	public Tuple getRecord(RID rid) throws InvalidSlotNumberException,
			InvalidTupleSizeException,HFBufMgrException, Exception {

		HFPage dirPage = new HFPage();
		PageId dirPageId = new PageId();
		HFPage dataPage = new HFPage();
		PageId dataPageId = new PageId();
		RID dirRecId = new RID();

		boolean found = findDataPage(rid, dirPageId, dirPage,dataPageId, dataPage, dirRecId);
		if (found != true)return null; // record not found

		Tuple dataTuple = new Tuple();
		dataTuple = dataPage.getRecord(rid);

		unpinPage(dataPageId, false);
		unpinPage(dirPageId, false);

		return dataTuple;
	}

	public Scan openScan() throws InvalidTupleSizeException, IOException {
		Scan scan = new Scan(this);
		return scan;
	}

	public void deleteFile() throws InvalidSlotNumberException,
	InvalidTupleSizeException,HFBufMgrException, HFDiskMgrException, IOException {

		PageId dirPageId = headerId;
		PageId nextDirPageId = new PageId();
		HFPage dirPage = new HFPage();
		RID dirRecId;
		Tuple pageDscTuple;
		PageDsc pageDsc;

		while (dirPageId.pid != INVALID_PAGE) {
			pinPage(dirPageId, dirPage, false);
			for (dirRecId = dirPage.firstRecord(); dirRecId != null; dirRecId = dirPage.nextRecord(dirRecId)) {
				pageDscTuple = dirPage.getRecord(dirRecId);
				pageDsc = new PageDsc(pageDscTuple);

				freePage(pageDsc.pageId); //delete dataPage
			}
			
			nextDirPageId = dirPage.getNextPage();
			freePage(dirPageId);

			dirPageId.pid = nextDirPageId.pid;
		}
		delete_file_entry(fileName);
	}


	
	private HFPage newDataPage(PageDsc pageDsc) throws HFException,
			HFBufMgrException, IOException {
		Page newPage = new Page();
		PageId newPageId = new PageId();
		newPageId = newPage(newPage, 1);

		HFPage newHFPage = new HFPage();
		newHFPage.init(newPageId, newPage);

		pageDsc.pageId.pid = newPageId.pid;
		pageDsc.recNum = 0;
		pageDsc.availableSpace = newHFPage.available_space();

		return newHFPage;
	}

	private void newDirPage(HFPage dirPage, PageId dirPageId)throws HFBufMgrException,IOException {
		HFPage nextDirPage = new HFPage();
		Page newPage = new Page();
		PageId nextDirPageId =  newPage(newPage, 1);

		// initialize new directory page
		nextDirPage.init(nextDirPageId, newPage);
		nextDirPage.setNextPage(new PageId(INVALID_PAGE));
		nextDirPage.setPrevPage(dirPageId);

		// update current directory page and unpin it
		dirPage.setNextPage(nextDirPageId);
		unpinPage(dirPageId, true);

		dirPageId.pid = nextDirPageId.pid;
		dirPage = nextDirPage;
	}

	private boolean findDataPage(RID rid, PageId dirPageId, HFPage dirpage,
			PageId dataPageId, HFPage datapage, RID dirRecId)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			HFBufMgrException, Exception {
		
		PageId curDirPageId = new PageId(headerId.pid);
		HFPage curDirPage = new HFPage();
		HFPage curDataPage = new HFPage();
		RID curDirRecId = new RID();
		PageId nextDirPageId = new PageId();

		PageDsc pageDsc;
		Tuple pageDscTuple;

		while (curDirPageId.pid != INVALID_PAGE) {
			pinPage(curDirPageId, curDirPage, false); // read from buffer

			for (curDirRecId = curDirPage.firstRecord(); curDirRecId != null; curDirRecId = curDirPage.nextRecord(curDirRecId)) {

				pageDscTuple = curDirPage.getRecord(curDirRecId);
				pageDsc = new PageDsc(pageDscTuple);

				try {
					pinPage(pageDsc.pageId, curDataPage, false);
				} catch (Exception e) { // error in pinning(invalid id) >> unpin currentDirPage
					unpinPage(curDirPageId, false);
					throw e;
				}

				if (pageDsc.pageId.pid == rid.pageNo.pid) { // page is found

					dirpage.setpage(curDirPage.getpage());
					dirPageId.pid = curDirPageId.pid;

					datapage.setpage(curDataPage.getpage());
					dataPageId.pid = pageDsc.pageId.pid;

					dirRecId.pageNo.pid = curDirRecId.pageNo.pid;
					dirRecId.slotNo = curDirRecId.slotNo;
					return true;

				} else { // page not found
					unpinPage(pageDsc.pageId, false);
				}

			}
			// page not found in current dir page
			nextDirPageId = curDirPage.getNextPage();
			unpinPage(curDirPageId, false);
			curDirPageId.pid = nextDirPageId.pid;
		}
		return false;
	}
	
	
	
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
		}
	} 
	
	private void unpinPage(PageId pageno, boolean dirty)
			throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
		}
	}

	private void freePage(PageId pageno) throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: freePage() failed");
		}
	}

	private PageId newPage(Page page, int num) throws HFBufMgrException {
		PageId tmpId = new PageId();
		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page, num);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
		}
		return tmpId;
	}

	private PageId get_file_entry(String filename) throws HFDiskMgrException {
		PageId tmpId = new PageId();
		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"Heapfile.java: get_file_entry() failed");
		}
		return tmpId;
	}

	private void add_file_entry(String filename, PageId pageno)
			throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"Heapfile.java: add_file_entry() failed");
		}
	}

	private void delete_file_entry(String filename) throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"Heapfile.java: delete_file_entry() failed");
		}
	}

}