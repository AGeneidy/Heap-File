package heap;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;

public class Scan implements GlobalConst {

	private Heapfile _hf;

	private PageId dirpageId = new PageId();

	private HFPage dirpage = new HFPage();

	private RID datapageRid = new RID();

	private PageId datapageId = new PageId();

	private HFPage datapage = new HFPage();

	private RID userrid = new RID();

	private boolean nextUserStatus;

	public Scan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		init(hf);
	}

	public Tuple getNext(RID rid) throws InvalidTupleSizeException, IOException {
		Tuple recptrtuple = null;

		if (nextUserStatus != true) {
			nextDataPage();
		}

		if (datapage == null)
			return null;

		rid.pageNo.pid = userrid.pageNo.pid;
		rid.slotNo = userrid.slotNo;

		try {
			recptrtuple = datapage.getRecord(rid);
		}

		catch (Exception e) {
			e.printStackTrace();
		}

		userrid = datapage.nextRecord(rid);
		if (userrid == null)
			nextUserStatus = false;
		else
			nextUserStatus = true;

		return recptrtuple;
	}

	public boolean position(RID rid) throws InvalidTupleSizeException,
			IOException {
		RID nxtrid = new RID();
		boolean bst;

		bst = peekNext(nxtrid);

		if (nxtrid.equals(rid) == true)
			return true;

		PageId pgid = new PageId();
		pgid.pid = rid.pageNo.pid;

		if (!datapageId.equals(pgid)) {

			reset();

			bst = firstDataPage();

			if (bst != true)
				return bst;

			while (!datapageId.equals(pgid)) {
				bst = nextDataPage();
				if (bst != true)
					return bst;
			}
		}


		try {
			userrid = datapage.firstRecord();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (userrid == null) {
			bst = false;
			return bst;
		}

		bst = peekNext(nxtrid);

		while ((bst == true) && (nxtrid != rid))
			bst = mvNext(nxtrid);

		return bst;
	}

	private void init(Heapfile hf) throws InvalidTupleSizeException,
			IOException {
		_hf = hf;

		firstDataPage();
	}

	public void closescan() {
		reset();
	}

	private void reset() {

		if (datapage != null) {

			try {
				unpinPage(datapageId, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		datapageId.pid = 0;
		datapage = null;

		if (dirpage != null) {

			try {
				unpinPage(dirpageId, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		dirpage = null;

		nextUserStatus = true;

	}

	private boolean firstDataPage() throws InvalidTupleSizeException,
			IOException {
		PageDsc dpinfo;
		Tuple rectuple = null;
		Boolean bst;


		dirpageId.pid = _hf.headerId.pid;
		nextUserStatus = true;

		try {
			dirpage = new HFPage();
			pinPage(dirpageId, (Page) dirpage, false);
		}

		catch (Exception e) {
			e.printStackTrace();
		}

		datapageRid = dirpage.firstRecord();
		if (datapageRid != null) {
			try {
				rectuple = dirpage.getRecord(datapageRid);
			}

			catch (Exception e) {
				e.printStackTrace();
			}

			dpinfo = new PageDsc(rectuple);
			datapageId.pid = dpinfo.pageId.pid;

		} else {

			PageId nextDirPageId = new PageId();

			nextDirPageId = dirpage.getNextPage();

			if (nextDirPageId.pid != INVALID_PAGE) {

				try {
					unpinPage(dirpageId, false);
					dirpage = null;
				}

				catch (Exception e) {
					e.printStackTrace();
				}

				try {

					dirpage = new HFPage();
					pinPage(nextDirPageId, (Page) dirpage, false);

				}

				catch (Exception e) {
					e.printStackTrace();
				}

				try {
					datapageRid = dirpage.firstRecord();
				}

				catch (Exception e) {
					e.printStackTrace();
					datapageId.pid = INVALID_PAGE;
				}

				if (datapageRid != null) {

					try {

						rectuple = dirpage.getRecord(datapageRid);
					}

					catch (Exception e) {
						e.printStackTrace();
					}

					if (rectuple.getLength() != PageDsc.size)
						return false;

					dpinfo = new PageDsc(rectuple);
					datapageId.pid = dpinfo.pageId.pid;

				} else {
					datapageId.pid = INVALID_PAGE;
				}
			}
			else {
				datapageId.pid = INVALID_PAGE;
			}
		}

		datapage = null;

		try {
			nextDataPage();
		}

		catch (Exception e) {
			e.printStackTrace();
		}

		return true;

	}

	private boolean nextDataPage() throws InvalidTupleSizeException,
			IOException {
		PageDsc dpinfo;

		boolean nextDataPageStatus;
		PageId nextDirPageId = new PageId();
		Tuple rectuple = null;

		if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
			return false;

		if (datapage == null) {
			if (datapageId.pid == INVALID_PAGE) {

				try {
					unpinPage(dirpageId, false);
					dirpage = null;
				} catch (Exception e) {
					e.printStackTrace();
				}

			} else {

				try {
					datapage = new HFPage();
					pinPage(datapageId, (Page) datapage, false);
				} catch (Exception e) {
					e.printStackTrace();
				}

				try {
					userrid = datapage.firstRecord();
				} catch (Exception e) {
					e.printStackTrace();
				}

				return true;
			}
		}

		try {
			unpinPage(datapageId, false);
			datapage = null;
		} catch (Exception e) {

		}

		if (dirpage == null) {
			return false;
		}

		datapageRid = dirpage.nextRecord(datapageRid);

		if (datapageRid == null) {
			nextDataPageStatus = false;
			nextDirPageId = dirpage.getNextPage();
			try {
				unpinPage(dirpageId, false);
				dirpage = null;

				datapageId.pid = INVALID_PAGE;
			}

			catch (Exception e) {

			}

			if (nextDirPageId.pid == INVALID_PAGE)
				return false;
			else {
				dirpageId = nextDirPageId;

				try {
					dirpage = new HFPage();
					pinPage(dirpageId, (Page) dirpage, false);
				}

				catch (Exception e) {

				}

				if (dirpage == null)
					return false;

				try {
					datapageRid = dirpage.firstRecord();
					nextDataPageStatus = true;
				} catch (Exception e) {
					nextDataPageStatus = false;
					return false;
				}
			}
		}

		try {
			rectuple = dirpage.getRecord(datapageRid);
		}

		catch (Exception e) {
			System.err.println("HeapFile: Error in Scan" + e);
		}

		if (rectuple.getLength() != PageDsc.size)
			return false;

		dpinfo = new PageDsc(rectuple);
		datapageId.pid = dpinfo.pageId.pid;

		try {
			datapage = new HFPage();
			pinPage(dpinfo.pageId, (Page) datapage, false);
		}

		catch (Exception e) {
			System.err.println("HeapFile: Error in Scan" + e);
		}

		userrid = datapage.firstRecord();

		if (userrid == null) {
			nextUserStatus = false;
			return false;
		}

		return true;
	}

	private boolean peekNext(RID rid) {

		rid.pageNo.pid = userrid.pageNo.pid;
		rid.slotNo = userrid.slotNo;
		return true;

	}

	private boolean mvNext(RID rid) throws InvalidTupleSizeException,
			IOException {
		RID nextrid;
		boolean status;

		if (datapage == null)
			return false;

		nextrid = datapage.nextRecord(rid);

		if (nextrid != null) {
			userrid.pageNo.pid = nextrid.pageNo.pid;
			userrid.slotNo = nextrid.slotNo;
			return true;
		} else {

			status = nextDataPage();

			if (status == true) {
				rid.pageNo.pid = userrid.pageNo.pid;
				rid.slotNo = userrid.slotNo;
			}

		}
		return true;
	}

	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
		}

	} 

	private void unpinPage(PageId pageno, boolean dirty)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
		}

	}

}
