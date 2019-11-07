#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

HT_ErrorCode HT_Init() {
  //insert code here
  return HT_OK;
}

#define OFFSET_1 sizeof(char)
#define OFFSET_2 sizeof(char)+sizeof(int)

HT_ErrorCode HT_CreateIndex(const char *fileName, int buckets) {
  /**
   * One implementation:
   * 	- Preallocate a number of 'buckets', for the initial hashing, which we will call our map.
   * 	- Any new 'bucket' will go below the map.
   * When a new record comes, we only have to find its hashing value, and add it to the corresponding bucket.
   * Conveniently the hashing value will be the same as the number of the bucket.
   * If that bucket is full, a new block will be allocated at the end and its number(int) will be added to the last one.
   */
  int file_desc;
  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &file_desc));

  BF_Block *block;
  BF_Block_Init (&block);

  char *data;
  CALL_BF(BF_AllocateBlock(file_desc, block));
  data = BF_Block_GetData(block);
  // Set an identifier('*') at the start to indicate that the file is a hashfile. 
  char h = '*';
  memcpy(data, &h, sizeof(char));
  // Save the number of buckets in the file.
  memcpy(data+OFFSET_1, &buckets, sizeof(int));
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  
  int zero = 0;
  // Allocate as many blocks as the buckets, which will give us our starting map. Save the number of records at the start of each block (zero at creation).
  for (int i = 0; i < buckets; i++) {
  	CALL_BF(BF_AllocateBlock(file_desc, block));
	data = BF_Block_GetData(block);
	memcpy(data, &zero, sizeof(int));
	BF_Block_SetDirty(block);
	CALL_BF(BF_UnpinBlock(block));
  }
  CALL_BF(BF_CloseFile(file_desc));
  /**/
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  /**
   * Opening a file requires validation that the file is indeed a hashfile.
   * The only thing that is needed is a check for the '*' identifier at the start.
   */
  CALL_BF(BF_OpenFile(fileName, indexDesc));

  char *data;
  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(*indexDesc, 0, block));
  data = BF_Block_GetData(block);
   
  if (*data != '*'){
  	return HT_ERROR;
  }
  CALL_BF(BF_UnpinBlock(block));
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {
  //insert code here
  /**
   * CloseFile just needs to close the file in the indexDesc position.
   */
  CALL_BF(BF_CloseFile(indexDesc));
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  //insert code here
  return HT_OK;
}
