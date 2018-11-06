/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  for (int i = 0; i < numBufs; i++){//flush dirty pages into files
    if (bufDescTable[i].dirty == true){
      (*(bufDescTable[i].file)).writePage(bufPool[i])
    }
  }

  //deallocate buf poll, buf desctable and hash table
  bufPool = NULL;
  bufDescTable = NULL;
  hashTable.~BufHashTbl();
}

void BufMgr::advanceClock()
{
  //advance the clock to next frame
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    // go through all the frames to check
    // whether there is a bufferexceed exception
    pinned = true;
    for (int i = 0; i < numBufs; i++)
      if (bufDescTable[i].pinCnt == 0)
        pinned = false;
    
    if (pinned) throw BufferExceededException;
    
    //if there is no exception
    while (true) {
      // if there is no valid page in frame
      if (!bufDescTable[frame].valid) {
        break;
      } else {
        // if there is a valid page in frame
        if (bufDescTable[frame].refbit){
          // if frame is used recently,turn it to false
          // and advanse
          bufDescTable[frame].refbit = false;
        } else {
          // if pincount is zero, the frame can be set
          //  else it should advance
          if (bufDescTable[frame].pinCnt == 0){
            //  check ditry, if dirty, flush
            if (bufDescTable[frame].dirty){
              (*(bufDescTable[frame].file)).writePage(bufPool[frame]);
            }
            break;
          }
        }
      }
      advanceClock();
    }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{

}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  //allocate a new page in the file
  badgerdb::Page new_page = (*file).allocaPage();
  //obtain next frame
  allocBuf();
  //add the relation to hash table
  hashTable.insert(file, pageNo, clockHand);
  //allocate the page to the frame
  bufDescTable.set(file, pageNo);
  //return the new page's page number
  pageNo = new_page.page_number();
  //return the pointer to the allocated page
  page = &(bufPool[clockHand]);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    for (int i = 0; i < numBufs; i++){
      //check if the page is in any frame
      if (bufDescTable[i].pageNo = pageNo) {
        //clear the frame with the page
        bufDescTable[i].Clear();
        //remove the relation in the hash table
        hashTable.remove(file, PageNo);
      }
    }
    //delete the page from file
    (*file).deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
